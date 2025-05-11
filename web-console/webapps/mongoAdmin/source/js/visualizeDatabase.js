
/**
 * Instance of vis.Network.
 * 
 * @type {object}
 */
MPG.visNetwork = null;

/**
 * Options of vis.Network.
 * 
 * @see https://ww3.arb.ca.gov/ei/tools/lib/vis/docs/network.html#Configuration_options
 * 
 * @type {object}
 */
MPG.visNetworkOptions = {

    width:  '100%',
    height: (window.innerHeight - 100) + 'px',
    nodes: {
        font: {
            color: "white"
        },
        color: {
            background: 'transparent',
            border: 'transparent'
        }
    },
    edges: {
        width: 1,
        color: {
            color: '#6eb72480',
            highlight: '#0062cc'
        },
    }

};

/**
 * Forwards navigation links.
 * 
 * @returns {void}
 */
MPG.helpers.forwardNavLinks = function() {

    var fragmentUrl = window.location.hash.split('#');

    if ( fragmentUrl.length === 2 && fragmentUrl[1] !== '' ) {

        databaseAndCollectionName = fragmentUrl[1].split('/');

        if ( databaseAndCollectionName.length === 1 ) {

            MPG.helpers.completeNavLinks('#' + databaseAndCollectionName[0]);

        } else if ( databaseAndCollectionName.length === 2 ) {

            MPG.helpers.completeNavLinks(
                '#' + databaseAndCollectionName[0] + '/' + databaseAndCollectionName[1]
            );

        }

    }

};

MPG.helpers.enumCollectionFields = function(databaseName,collectionName) {

    MPG.collectionFields = [];

    var requestBody = {
        'databaseName': databaseName,
        'collectionName': collectionName
    };

    MPG.helpers.doAjaxRequest(
        'POST',
        './enumCollectionFields',
        function(response) {

            JSON.parse(response).forEach(function(collectionField) {
                if ( typeof collectionField === 'string' ) {
                    MPG.collectionFields.push(collectionField);
                }
            });

            var infoDiv = document.querySelector('#info');
            var innerHTML = '<h3>Collection Fields</h3><table class="table table-bordered" >';

            MPG.collectionFields.forEach(function(collectionField) {
                innerHTML += '<tr>';
                innerHTML += '<td title="' + collectionField + '">'
                    + collectionField + '</td>';
                innerHTML += '</tr>';

            });
            
            innerHTML += '</table>';

            infoDiv.innerHTML = innerHTML;
            infoDiv.style.display = 'block';          

        },
        JSON.stringify(requestBody)
    );


}
/**
 * Draws vis.Network graph.
 * 
 * @returns {void}
 */
MPG.drawVisNetwork = function() {

    MPG.helpers.doAjaxRequest(
        'GET',
        './getDatabaseGraph',
        function(response) {

            var visNetworkContainer = document.querySelector('#vis-network-container');
            var networkGraph = JSON.parse(response);

            MPG.visNetwork = new vis.Network(
                visNetworkContainer, networkGraph.visData, MPG.visNetworkOptions
            );

            MPG.visNetwork.on('select', function(nodeProperties) {
                
                if ( nodeProperties.nodes.length === 0 ) {
                    var infoDiv = document.getElementById('info');
                    infoDiv.style.display = 'none';
                    return;
                }

                var selectedNodeId = nodeProperties.nodes[0];
                var selectedNodeMapping = networkGraph.mapping[selectedNodeId];                

                if ( selectedNodeMapping.databaseName !== null ) {

                    if ( selectedNodeMapping.collectionName !== null ) {
                        MPG.helpers.enumCollectionFields(selectedNodeMapping.databaseName,selectedNodeMapping.collectionName);
                    }
                    else{    // dbStats  
                        var requestBody = {
                            'databaseName': selectedNodeMapping.databaseName,                            
                        };           
                        MPG.helpers.doAjaxRequest(
                            'POST',
                            './dbstats',
                            function(response) {
                                var infoDiv = document.getElementById('info');                              
                                infoDiv.innerHTML = response;
                            },
                            JSON.stringify(requestBody)
                        );
                    }
                    
                }

            });
            
            MPG.visNetwork.on('doubleClick', function(nodeProperties) {

                if ( nodeProperties.nodes.length === 0 ) {
                    return;
                }

                var selectedNodeId = nodeProperties.nodes[0];
                var selectedNodeMapping = networkGraph.mapping[selectedNodeId];
                var targetUrl = './queryDocuments#';

                if ( selectedNodeMapping.databaseName !== null ) {

                    targetUrl += selectedNodeMapping.databaseName;

                    if ( selectedNodeMapping.collectionName !== null ) {
                        targetUrl += '/' + selectedNodeMapping.collectionName;
                    }

                    window.location.href = targetUrl;

                }


            });

        },
        null
    );

};

// When document is ready:
window.addEventListener('DOMContentLoaded', function(_event) {

    MPG.helpers.forwardNavLinks();
    MPG.eventListeners.addMenuToggle();
    
    MPG.drawVisNetwork();

});
