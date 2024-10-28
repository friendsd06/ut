// Initialize Cytoscape
var cy = cytoscape({
    container: document.getElementById('cy'),
    elements: [], // Start with an empty graph
    style: [
        {
            selector: 'node',
            style: {
                'label': 'data(id)',
                'background-color': 'gray',
                'text-valign': 'center',
                'color': 'white',
                'text-outline-width': 2,
                'text-outline-color': '#555',
                'width': '50px',
                'height': '50px',
                'font-size': '14px'
            }
        },
        {
            selector: 'edge',
            style: {
                'width': 2,
                'line-color': '#ccc',
                'target-arrow-color': '#ccc',
                'target-arrow-shape': 'triangle',
                'curve-style': 'bezier'
            }
        }
    ],
    layout: {
        name: 'dagre',
        rankDir: 'TB', // Top to Bottom
        nodeSep: 50,
        rankSep: 75
    }
});

// Establish WebSocket connection
var wsProtocol = window.location.protocol === 'https:' ? 'wss://' : 'ws://';
var wsUrl = wsProtocol + window.location.hostname + ':' + window.location.port + '/dagupdates';
var ws = new WebSocket(wsUrl);

ws.onopen = function () {
    console.log('WebSocket connection established');
};

ws.onmessage = function (event) {
    console.log('Received message:', event.data);
    try {
        var message = JSON.parse(event.data);
        handleUpdate(message);
    } catch (e) {
        console.error('Error parsing message:', e);
    }
};

ws.onerror = function (error) {
    console.error('WebSocket error:', error);
};

ws.onclose = function () {
    console.log('WebSocket connection closed');
};

// Function to handle incoming messages from the backend
function handleUpdate(message) {
    console.log('Handling update:', message);
    if (message.type === 'graph') {
        // Initialize graph structure
        cy.add(message.elements);
        cy.layout({ name: 'dagre' }).run();
    } else if (message.type === 'status') {
        // Update node status
        var node = cy.getElementById(message.nodeId);
        if (node && node.length > 0) {
            node.style('background-color', getColorForStatus(message.status));
        } else {
            console.warn('Node not found:', message.nodeId);
        }
    } else if (message.type === 'dagCompleted') {
        alert('DAG execution completed in ' + message.executionTime + 'ms');
    } else {
        console.warn('Unknown message type:', message.type);
    }
}

// Function to determine the color of a node based on its status
function getColorForStatus(status) {
    switch (status) {
        case 'PENDING':
            return 'gray';
        case 'RUNNING':
            return 'orange';
        case 'COMPLETED':
            return 'green';
        case 'FAILED':
            return 'red';
        default:
            return 'gray';
    }
}
