let cy = null;

document.addEventListener('DOMContentLoaded', function() {
    if (!window.cytoscape || !window.dagre || !window.cytoscapeDagre) {
        console.error('Required libraries are not loaded');
        return;
    }

    cytoscape.use(cytoscapeDagre);
    initializeCytoscape();
    initializeWebSocket();
});

function initializeCytoscape() {
    try {
        cy = cytoscape({
            container: document.getElementById('cy'),
            elements: [],
            style: [
                {
                    selector: 'node',
                    style: {
                        'label': 'data(id)',
                        'background-color': '#666',
                        'text-valign': 'center',
                        'text-halign': 'center',
                        'color': 'white',
                        'text-outline-width': 2,
                        'text-outline-color': '#555',
                        'width': 60,
                        'height': 60,
                        'font-size': '14px',
                        'font-weight': 'bold'
                    }
                },
                {
                    selector: 'edge',
                    style: {
                        'width': 3,
                        'line-color': '#999',
                        'target-arrow-color': '#999',
                        'target-arrow-shape': 'triangle',
                        'curve-style': 'bezier',
                        'arrow-scale': 1.5,
                        'label': 'data(id)',
                        'font-size': '12px'
                    }
                },
                {
                    selector: 'node[status = "PENDING"]',
                    style: {
                        'background-color': '#666'
                    }
                },
                {
                    selector: 'node[status = "RUNNING"]',
                    style: {
                        'background-color': '#FFA500'
                    }
                },
                {
                    selector: 'node[status = "COMPLETED"]',
                    style: {
                        'background-color': '#4CAF50'
                    }
                },
                {
                    selector: 'node[status = "FAILED"]',
                    style: {
                        'background-color': '#F44336'
                    }
                }
            ],
            layout: {
                name: 'dagre',
                rankDir: 'LR',
                nodeSep: 80,
                rankSep: 100,
                fit: true,
                padding: 50,
                animate: true,
                animationDuration: 500
            }
        });

        // Add click handler for nodes
        cy.on('tap', 'node', function(evt) {
            const node = evt.target;
            console.log('Node clicked:', node.id(), 'Status:', node.data('status'));
            addStatusMessage(`Node ${node.id()} clicked - Status: ${node.data('status') || 'PENDING'}`);
        });

    } catch (error) {
        console.error('Error initializing Cytoscape:', error);
        addStatusMessage('Error initializing graph');
    }
}

function initializeWebSocket() {
    const wsProtocol = window.location.protocol === 'https:' ? 'wss://' : 'ws://';
    const wsUrl = `${wsProtocol}${window.location.hostname}:4567/dagupdates`;

    console.log('Connecting to WebSocket:', wsUrl);

    const ws = new WebSocket(wsUrl);

    ws.onopen = function() {
        console.log('WebSocket connected');
        addStatusMessage('Connected to server');
    };

    ws.onmessage = function(event) {
        console.log('Received message:', event.data);
        try {
            const message = JSON.parse(event.data);

            if (message.type === 'graph' && message.elements) {
                console.log('Received graph data');
                updateGraph(message.elements);
                addStatusMessage('Graph initialized');
            } else if (message.type === 'status') {
                console.log('Received status update for node:', message.nodeId);
                updateNodeStatus(message.nodeId, message.status);
                addStatusMessage(`Node ${message.nodeId} status: ${message.status}`);
            }
        } catch (error) {
            console.error('Error processing message:', error);
            addStatusMessage('Error processing update');
        }
    };

    ws.onerror = function(error) {
        console.error('WebSocket error:', error);
        addStatusMessage('Connection error');
    };

    ws.onclose = function() {
        console.log('WebSocket disconnected. Reconnecting...');
        addStatusMessage('Disconnected. Reconnecting...');
        setTimeout(initializeWebSocket, 2000);
    };
}

function updateGraph(elements) {
    if (!cy || !elements) return;

    try {
        // Remove existing elements
        cy.elements().remove();

        // Add new elements with initial PENDING status for nodes
        const processedElements = elements.map(element => ({
            data: {
                ...element.data,
                // Only add status to nodes (elements without source/target)
                status: !element.data.source ? 'PENDING' : undefined
            }
        }));

        // Add elements to the graph
        cy.add(processedElements);

        // Run layout
        const layout = cy.layout({
            name: 'dagre',
            rankDir: 'LR',
            nodeSep: 80,
            rankSep: 100,
            fit: true,
            padding: 50,
            animate: true,
            animationDuration: 500
        });

        layout.run();
        cy.fit();

        addStatusMessage(`Graph initialized with ${cy.nodes().length} nodes and ${cy.edges().length} edges`);

    } catch (error) {
        console.error('Error updating graph:', error);
        addStatusMessage('Error updating graph');
    }
}

function updateNodeStatus(nodeId, status) {
    if (!cy || !nodeId) return;

    try {
        const node = cy.getElementById(nodeId);
        if (node.length > 0) {
            // Update node status
            node.data('status', status);

            // Update connected edges based on status
            if (status === 'RUNNING') {
                // Highlight incoming edges
                node.incomers('edge').style({
                    'line-color': '#FFA500',
                    'target-arrow-color': '#FFA500'
                });
            } else if (status === 'COMPLETED') {
                // Highlight outgoing edges
                node.outgoers('edge').style({
                    'line-color': '#4CAF50',
                    'target-arrow-color': '#4CAF50'
                });
            } else if (status === 'FAILED') {
                // Show failed state
                node.connectedEdges().style({
                    'line-color': '#F44336',
                    'target-arrow-color': '#F44336'
                });
            }

            addStatusMessage(`Updated node ${nodeId} to status: ${status}`);
        } else {
            console.warn('Node not found:', nodeId);
            addStatusMessage(`Node ${nodeId} not found`);
        }
    } catch (error) {
        console.error('Error updating node status:', error);
        addStatusMessage('Error updating node status');
    }
}

function addStatusMessage(message) {
    const statusDiv = document.getElementById('status-messages');
    if (statusDiv) {
        const messageElement = document.createElement('div');
        messageElement.textContent = `${new Date().toLocaleTimeString()}: ${message}`;
        statusDiv.insertBefore(messageElement, statusDiv.firstChild);

        // Keep only last 10 messages
        while (statusDiv.children.length > 10) {
            statusDiv.removeChild(statusDiv.lastChild);
        }
    }
}

window.resetView = function() {
    if (cy) {
        cy.fit();
        cy.center();
        addStatusMessage('View reset');
    }
};