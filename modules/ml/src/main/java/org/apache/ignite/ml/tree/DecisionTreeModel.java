package org.apache.ignite.ml.tree;

/**
 * Base class for decision tree models.
 */
public class DecisionTreeModel implements IgniteModel<Vector, Double>, JSONWritable, JSONReadable, PMMLWritable, PMMLReadable {
    /**
     * Root node.
     */
    private DecisionTreeNode rootNode;

    /**
     * Creates the model.
     *
     * @param rootNode Root node of the tree.
     */
    public DecisionTreeModel(DecisionTreeNode rootNode) {
        this.rootNode = rootNode;
    }

    public DecisionTreeModel() {

    }

    /**
     * Returns the root node.
     */
    public DecisionTreeNode getRootNode() {
        return rootNode;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Double predict(Vector features) {
        return rootNode.predict(features);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return toString(false);
    }

    /** {@inheritDoc} */
    @Override public String toString(boolean pretty) {
        return DecisionTreeTrainer.printTree(rootNode, pretty);
    }

    @Override
    public DecisionTreeModel fromJSON(Path path) {
            ObjectMapper mapper = new ObjectMapper();
            DecisionTreeModel mdl;
            try {
                mdl = mapper.readValue(new File(path.toAbsolutePath().toString()), DecisionTreeModel.class);

                return mdl;
            } catch (IOException e) {
                e.printStackTrace();
            }
        return null;
    }



    private DecisionTreeNode buildTree(Node node) {
        Predicate predicate = node.getPredicate();

        if (node.hasNodes()) {
            Node leftNode = null;
            Node rightNode = null;
            for (int i = 0; i < node.getNodes().size(); i++) {
                if(node.getNodes().get(i).getId().equals("left")) {
                    leftNode = node.getNodes().get(i);
                } else if(node.getNodes().get(i).getId().equals("right")) {
                    rightNode = node.getNodes().get(i);
                } else {
                    // TODO: we couldn't handle this case left or right
                }
            }

            int featureIdx = Integer.parseInt(((SimplePredicate)predicate).getField().getValue());
            double threshold = Double.parseDouble(((SimplePredicate)predicate).getValue());

            // TODO: correctly handle missing nodes, add test for that
            String defaultChild = node.getDefaultChild();
            if(defaultChild!= null && !defaultChild.isEmpty()) {
                double missingNodevalue = Double.parseDouble(defaultChild);
                DecisionTreeLeafNode missingNode = new DecisionTreeLeafNode(missingNodevalue);
                return new DecisionTreeConditionalNode(featureIdx, threshold, buildTree(rightNode), buildTree(leftNode), missingNode);
            }
            return new DecisionTreeConditionalNode(featureIdx, threshold, buildTree(rightNode), buildTree(leftNode), null);
        } else {
            return new DecisionTreeLeafNode(Double.parseDouble(node.getScore()));
        }

    }

    private Node buildPmmlTree(DecisionTreeNode node, Predicate predicate) {
        Node pmmlNode = new Node();
        pmmlNode.setPredicate(predicate);

        if (node instanceof DecisionTreeConditionalNode) {
            DecisionTreeConditionalNode splitNode = ((DecisionTreeConditionalNode) node);

            if (splitNode.getMissingNode() != null) {

                DecisionTreeLeafNode missingNode = ((DecisionTreeLeafNode)splitNode.getMissingNode());
                pmmlNode.setDefaultChild(String.valueOf(missingNode.getVal()));
            }

            DecisionTreeNode leftNode = splitNode.getElseNode();
            if(leftNode != null) {
                Predicate leftPredicate = getPredicate(leftNode, true);
                Node leftPmmlNode = buildPmmlTree(leftNode, leftPredicate);
                leftPmmlNode.setId("left");
                pmmlNode.addNodes(leftPmmlNode);
            }

            DecisionTreeNode rightNode = splitNode.getThenNode();
            if(rightNode != null) {
                Predicate rightPredicate = getPredicate(rightNode, false);
                Node rightPmmlNode = buildPmmlTree(rightNode, rightPredicate);
                rightPmmlNode.setId("right");
                pmmlNode.addNodes(rightPmmlNode);
            }
        } else if (node instanceof DecisionTreeLeafNode) {
            DecisionTreeLeafNode leafNode = ((DecisionTreeLeafNode) node);
            pmmlNode.setScore(String.valueOf(leafNode.getVal()));
        }

        return pmmlNode;
    }

    private Predicate getPredicate(DecisionTreeNode node, boolean isLeft) {
        if (node instanceof DecisionTreeConditionalNode) {
            DecisionTreeConditionalNode splitNode = ((DecisionTreeConditionalNode) node);

            FieldName fieldName = FieldName.create(String.valueOf(splitNode.getCol()));

            String threshold = String.valueOf(splitNode.getThreshold());

            if (isLeft) {
                return new SimplePredicate(fieldName, SimplePredicate.Operator.LESS_OR_EQUAL)
                        .setValue(threshold);
            } else {
                return new SimplePredicate(fieldName, SimplePredicate.Operator.GREATER_THAN)
                        .setValue(threshold);
            }
        } else {
            return new True();
        }
    }


}