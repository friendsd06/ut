let cy = null;

document.addEventListener('DOMContentLoaded', function() {
    // First verify that all required libraries are loaded
    if (!window.cytoscape || !window.dagre || !window.cytoscapeDagre) {
        console.error('Required libraries are not loaded');
        return;
    }

    // Register the dagre layout
    cytoscape.use(cytoscapeDagre);

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
                        'font-size': '12px',
                        'text-rotation': 'autorotate'
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
                rankDir: 'LR', // Left to Right layout
                nodeSep: 80,
                rankSep: 100,
                fit: true,
                padding: 50,
                animate: true,
                animationDuration: 500
            }
        });

        initializeWebSocket();
    } catch (error) {
        console.error('Error initializing Cytoscape:', error);
    }
});

function initializeWebSocket() {
    const wsProtocol = window.location.protocol === 'https:' ? 'wss://' : 'ws://';
    const wsUrl = `${wsProtocol}localhost:4567/dagupdates`;

    console.log('Connecting to WebSocket:', wsUrl);

    const ws = new WebSocket(wsUrl);

    ws.onopen = function() {
        console.log('WebSocket connected');
    };

    ws.onmessage = function(event) {
        try {
            const message = JSON.parse(event.data);
            handleUpdate(message);
        } catch (error) {
            console.error('Error processing message:', error);
        }
    };

    ws.onerror = function(error) {
        console.error('WebSocket error:', error);
    };

    ws.onclose = function() {
        console.log('WebSocket disconnected. Reconnecting in 2s...');
        setTimeout(initializeWebSocket, 2000);
    };
}

function handleUpdate(message) {
    if (!cy) return;

    try {
        if (Array.isArray(message)) {
            // Initial graph data
            updateGraph(message);
        } else if (message.type === 'status') {
            // Status update
            updateNodeStatus(message.nodeId, message.status);
        }
    } catch (error) {
        console.error('Error handling update:', error);
    }
}

function updateGraph(elements) {
    if (!cy || !elements) return;

    try {
        cy.elements().remove();
        cy.add(elements.map(element => ({
            data: {
                ...element.data,
                status: 'PENDING' // Set initial status
            }
        })));

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

        // Add click handler for nodes
        cy.on('tap', 'node', function(evt) {
            const node = evt.target;
            console.log('Node clicked:', node.id(), 'Status:', node.data('status'));
        });

    } catch (error) {
        console.error('Error updating graph:', error);
    }
}

function updateNodeStatus(nodeId, status) {
    if (!cy || !nodeId) return;

    try {
        const node = cy.getElementById(nodeId);
        if (node.length > 0) {
            // Update node status in data and style
            node.data('status', status);

            // Highlight the incoming and outgoing edges
            const connectedEdges = node.connectedEdges();

            if (status === 'RUNNING') {
                connectedEdges.style({
                    'line-color': '#FFA500',
                    'target-arrow-color': '#FFA500'
                });
            } else if (status === 'COMPLETED') {
                const outgoingEdges = node.outgoers('edge');
                outgoingEdges.style({
                    'line-color': '#4CAF50',
                    'target-arrow-color': '#4CAF50'
                });
            }
        }
    } catch (error) {
        console.error('Error updating node status:', error);
    }
}

function getColorForStatus(status) {
    const colors = {
        'PENDING': '#666',
        'RUNNING': '#FFA500',
        'COMPLETED': '#4CAF50',
        'FAILED': '#F44336'
    };
    return colors[status] || '#666';
}

// Test function to simulate updates
window.testGraph = function() {
    const testData = [
        {"data": {"id": "A"}},
        {"data": {"id": "I"}},
        {"data": {"id": "B"}},
        {"data": {"id": "G"}},
        {"data": {"id": "H"}},
        {"data": {"id": "F"}},
        {"data": {"id": "D"}},
        {"data": {"id": "J"}},
        {"data": {"id": "E"}},
        {"data": {"id": "C"}},
        {"data": {"source": "A", "id": "A->B", "target": "B"}},
        {"data": {"source": "A", "id": "A->C", "target": "C"}},
        {"data": {"source": "I", "id": "I->J", "target": "J"}},
        {"data": {"source": "B", "id": "B->D", "target": "D"}},
        {"data": {"source": "G", "id": "G->H", "target": "H"}},
        {"data": {"source": "H", "id": "H->I", "target": "I"}},
        {"data": {"source": "F", "id": "F->G", "target": "G"}},
        {"data": {"source": "D", "id": "D->F", "target": "F"}},
        {"data": {"source": "D", "id": "D->E", "target": "E"}},
        {"data": {"source": "E", "id": "E->G", "target": "G"}},
        {"data": {"source": "C", "id": "C->D", "target": "D"}}
    ];

    handleUpdate(testData);

    // Simulate status updates
    let delay = 1000;
    const updateSequence = [
        { nodeId: 'A', status: 'RUNNING' },
        { nodeId: 'A', status: 'COMPLETED' },
        { nodeId: 'B', status: 'RUNNING' },
        { nodeId: 'C', status: 'RUNNING' },
        { nodeId: 'B', status: 'COMPLETED' },
        { nodeId: 'C', status: 'COMPLETED' },
        { nodeId: 'D', status: 'RUNNING' }
    ];

    updateSequence.forEach((update, index) => {
        setTimeout(() => {
            handleUpdate({ type: 'status', ...update });
        }, delay * (index + 1));
    });
};

window.resetView = function() {
    if (cy) {
        cy.fit();
        cy.center();
    }
};