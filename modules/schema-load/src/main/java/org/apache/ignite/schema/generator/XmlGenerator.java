/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.schema.generator;

import org.apache.ignite.lang.*;
import org.apache.ignite.schema.ui.*;
import org.gridgain.grid.cache.query.*;
import org.gridgain.grid.util.typedef.*;
import org.w3c.dom.*;

import javax.xml.parsers.*;
import javax.xml.transform.*;
import javax.xml.transform.dom.*;
import javax.xml.transform.stream.*;
import java.io.*;
import java.util.*;

import static org.apache.ignite.schema.ui.MessageBox.Result.*;

/**
 * Generator of XML files for type metadata.
 */
public class XmlGenerator {
    /**
     * Add bean to XML document.
     *
     * @param doc XML document.
     * @param parent Parent XML node.
     * @param clazz Bean class.
     */
    private static Element addBean(Document doc, Node parent, Class<?> clazz) {
        Element elem = doc.createElement("bean");

        elem.setAttribute("class", clazz.getName());

        parent.appendChild(elem);

        return elem;
    }

    /**
     * Add element to XML document.
     *
     * @param doc XML document.
     * @param parent Parent XML node.
     * @param tagName XML tag name.
     * @param attr1 Name for first attr.
     * @param val1 Value for first attribute.
     * @param attr2 Name for second attr.
     * @param val2 Value for second attribute.
     */
    private static Element addElement(Document doc, Node parent, String tagName,
        String attr1, String val1, String attr2, String val2) {
        Element elem = doc.createElement(tagName);

        if (attr1 != null)
            elem.setAttribute(attr1, val1);

        if (attr2 != null)
            elem.setAttribute(attr2, val2);

        parent.appendChild(elem);

        return elem;
    }

    /**
     * Add element to XML document.
     *
     * @param doc XML document.
     * @param parent Parent XML node.
     * @param tagName XML tag name.
     */
    private static Element addElement(Document doc, Node parent, String tagName) {
        return addElement(doc, parent, tagName, null, null, null, null);
    }

    /**
     * Add element to XML document.
     *
     * @param doc XML document.
     * @param parent Parent XML node.
     * @param tagName XML tag name.
     */
    private static Element addElement(Document doc, Node parent, String tagName, String attrName, String attrVal) {
        return addElement(doc, parent, tagName, attrName, attrVal, null, null);
    }

    /**
     * Add &quot;property&quot; element to XML document.
     *
     * @param doc XML document.
     * @param parent Parent XML node.
     * @param name Value for &quot;name&quot; attribute
     * @param val Value for &quot;value&quot; attribute
     */
    private static Element addProperty(Document doc, Node parent, String name, String val) {
        String valAttr = val != null ? "value" : null;

        return addElement(doc, parent, "property", "name", name, valAttr, val);
    }

    /**
     * Add fields to xml document.
     *
     * @param doc XML document.
     * @param parent Parent XML node.
     * @param name Property name.
     * @param fields Map with fields.
     */
    private static void addFields(Document doc, Node parent, String name, Map<String, Class<?>> fields) {
        if (!fields.isEmpty()) {
            Element prop = addProperty(doc, parent, name, null);

            Element map = addElement(doc, prop, "map");

            for (Map.Entry<String, Class<?>> item : fields.entrySet())
                addElement(doc, map, "entry", "key", item.getKey(), "value", item.getValue().getName());
        }
    }

    /**
     * Add type descriptors to XML document.
     *
     * @param doc XML document.
     * @param parent Parent XML node.
     * @param name Property name.
     * @param descs Map with type descriptors.
     */
    private static void addTypeDescriptors(Document doc, Node parent, String name,
        Collection<GridCacheQueryTypeDescriptor> descs) {
        if (!descs.isEmpty()) {
            Element prop = addProperty(doc, parent, name, null);

            Element list = addElement(doc, prop, "list");

            for (GridCacheQueryTypeDescriptor desc : descs) {
                Element item = addBean(doc, list, GridCacheQueryTypeDescriptor.class);

                addProperty(doc, item, "javaName", desc.getJavaName());
                addProperty(doc, item, "javaType", desc.getJavaType().getName());
                addProperty(doc, item, "dbName", desc.getDbName());
                addProperty(doc, item, "dbType", String.valueOf(desc.getDbType()));
            }
        }
    }

    /**
     * Add text fields to xml document.
     *
     * @param doc XML document.
     * @param parent Parent XML node.
     * @param textFields Collection with text fields.
     */
    private static void addTextFields(Document doc, Node parent, Collection<String> textFields) {
        if (!textFields.isEmpty()) {
            Element prop = addProperty(doc, parent, "textFields", null);

            Element list = addElement(doc, prop, "list");

            for (String textField : textFields)
                addElement(doc, list, "value").setNodeValue(textField);
        }
    }

    /**
     * Add indexes to xml document.
     *
     * @param doc XML document.
     * @param parent Parent XML node.
     * @param groups Map with indexes.
     */
    private static void addGroups(Document doc, Node parent,
        Map<String, LinkedHashMap<String, IgniteBiTuple<Class<?>, Boolean>>> groups) {
        if (!F.isEmpty(groups)) {
            Element prop = addProperty(doc, parent, "groups", null);

            Element map = addElement(doc, prop, "map");

            for (Map.Entry<String, LinkedHashMap<String, IgniteBiTuple<Class<?>, Boolean>>> group : groups.entrySet()) {
                Element entry1 = addElement(doc, map, "entry", "key", group.getKey());

                Element val1 = addElement(doc, entry1, "map");

                LinkedHashMap<String, IgniteBiTuple<Class<?>, Boolean>> fields = group.getValue();

                for (Map.Entry<String, IgniteBiTuple<Class<?>, Boolean>> field : fields.entrySet()) {
                    Element entry2 = addElement(doc, val1, "entry", "key", field.getKey());

                    Element val2 = addBean(doc, entry2, IgniteBiTuple.class);

                    IgniteBiTuple<Class<?>, Boolean> tuple = field.getValue();

                    Class<?> clazz = tuple.get1();

                    assert clazz != null;

                    addElement(doc, val2, "constructor-arg", null, null, "value", clazz.getName());
                    addElement(doc, val2, "constructor-arg", null, null, "value", String.valueOf(tuple.get2()));
                }
            }
        }
    }

    /**
     * Add element with type metadata to XML document.
     *
     * @param doc XML document.
     * @param parent Parent XML node.
     * @param pkg Package fo types.
     * @param meta Meta.
     */
    private static void addTypeMetadata(Document doc, Node parent, String pkg, GridCacheQueryTypeMetadata meta) {
        Element bean = addBean(doc, parent, GridCacheQueryTypeMetadata.class);

        addProperty(doc, bean, "type", pkg + "." + meta.getType());

        addProperty(doc, bean, "keyType", pkg + "." + meta.getKeyType());

        addProperty(doc, bean, "schema", meta.getSchema());

        addProperty(doc, bean, "tableName", meta.getTableName());

        addTypeDescriptors(doc, bean, "keyDescriptors", meta.getKeyDescriptors());

        addTypeDescriptors(doc, bean, "valueDescriptors", meta.getValueDescriptors());

        addFields(doc, bean, "queryFields", meta.getQueryFields());

        addFields(doc, bean, "ascendingFields", meta.getAscendingFields());

        addFields(doc, bean, "descendingFields", meta.getDescendingFields());

        addTextFields(doc, bean, meta.getTextFields());

        addGroups(doc, bean, meta.getGroups());
    }

    /**
     * Transform metadata into xml.
     *
     * @param pkg Package fo types.
     * @param meta Metadata to generate.
     * @param out File to output result.
     * @param askOverwrite Callback to ask user to confirm file overwrite.
     */
    public static void generate(String pkg, GridCacheQueryTypeMetadata meta, File out, ConfirmCallable askOverwrite) {
        generate(pkg, Collections.singleton(meta), out, askOverwrite);
    }

    /**
     * Transform metadata into xml.
     *
     * @param pkg Package fo types.
     * @param meta Metadata to generate.
     * @param out File to output result.
     * @param askOverwrite Callback to ask user to confirm file overwrite.
     */
    public static void generate(String pkg, Collection<GridCacheQueryTypeMetadata> meta, File out,
        ConfirmCallable askOverwrite) {
        try {
            if (out.exists()) {
                MessageBox.Result choice = askOverwrite.confirm(out.getName());

                if (CANCEL == choice)
                    throw new IllegalStateException("XML generation was canceled!");

                if (NO == choice || NO_TO_ALL == choice)
                    return;
            }

            DocumentBuilderFactory docFactory = DocumentBuilderFactory.newInstance();

            DocumentBuilder docBuilder = docFactory.newDocumentBuilder();

            Document doc = docBuilder.newDocument();
            doc.setXmlStandalone(true);

            Element beans = addElement(doc, doc, "beans");
            beans.setAttribute("xmlns", "http://www.springframework.org/schema/beans");
            beans.setAttribute("xmlns:xsi", "http://www.w3.org/2001/XMLSchema-instance");
            beans.setAttribute("xmlns:util", "http://www.springframework.org/schema/util");
            beans.setAttribute("xsi:schemaLocation",
                "http://www.springframework.org/schema/beans " +
                    "http://www.springframework.org/schema/beans/spring-beans.xsd " +
                    "http://www.springframework.org/schema/util " +
                    "http://www.springframework.org/schema/util/spring-util.xsd");

            for (GridCacheQueryTypeMetadata item : meta)
                addTypeMetadata(doc, beans, pkg, item);

            TransformerFactory transformerFactory = TransformerFactory.newInstance();

            Transformer transformer = transformerFactory.newTransformer();

            transformer.setOutputProperty(OutputKeys.INDENT, "yes");
            transformer.setOutputProperty("{http://xml.apache.org/xslt}indent-amount", "4");

            transformer.transform(new DOMSource(doc), new StreamResult(out));
        }
        catch (ParserConfigurationException | TransformerException e) {
            throw new IllegalStateException(e);
        }
    }
}
