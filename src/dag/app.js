// Graph state management
let cy = null;
let isDarkTheme = false;

// Node status configuration
const STATUS_COLORS = {
    PENDING: '#666666',
    RUNNING: '#FFA500',
    COMPLETED: '#4CAF50',
    FAILED: '#F44336'
};

document.addEventListener('DOMContentLoaded', () => {
    if (!window.cytoscape || !window.dagre || !window.cytoscapeDagre) {
        console.error('Required libraries not loaded');
        return;
    }

    cytoscape.use(cytoscapeDagre);
    initializeCytoscape();
    initializeWebSocket();
});

function initializeCytoscape() {
    cy = cytoscape({
        container: document.getElementById('cy'),
        elements: [],
        style: [
            {
                selector: 'node',
                style: {
                    'label': 'data(id)',
                    'background-color': STATUS_COLORS.PENDING,
                    'text-valign': 'center',
                    'text-halign': 'center',
                    'color': '#ffffff',
                    'text-outline-width': 2,
                    'text-outline-color': '#555555',
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
                    'line-color': '#999999',
                    'target-arrow-color': '#999999',
                    'target-arrow-shape': 'triangle',
                    'curve-style': 'bezier',
                    'arrow-scale': 1.5
                }
            }
        ],
        layout: {
            name: 'dagre',
            rankDir: 'LR',
            nodeSep: 80,
            rankSep: 100,
            padding: 50,
            animate: true,
            animationDuration: 300
        }
    });

    cy.on('tap', 'node', (evt) => {
        const node = evt.target;
        console.log('Node clicked:', node.id(), 'Status:', node.data('status'));
        addStatusMessage(`Node ${node.id()} clicked - Current status: ${node.data('status') || 'PENDING'}`);
    });
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
            },
            group: element.group
        }));

        // Add elements to the graph
        cy.add(processedElements);

        // Apply layout
        cy.layout({
            name: 'dagre',
            rankDir: 'LR',
            nodeSep: 80,
            rankSep: 100,
            padding: 50,
            animate: true,
            animationDuration: 300
        }).run();

        // Set initial colors based on status
        cy.nodes().forEach(node => {
            const status = node.data('status') || 'PENDING';
            node.style('background-color', STATUS_COLORS[status]);
        });

        addStatusMessage(`Graph initialized with ${cy.nodes().length} nodes`);
    } catch (error) {
        console.error('Error updating graph:', error);
        addStatusMessage('Error updating graph', 'FAILED');
    }
}

function initializeWebSocket() {
    const wsProtocol = window.location.protocol === 'https:' ? 'wss://' : 'ws://';
    const wsUrl = `${wsProtocol}${window.location.hostname}:4567/dagupdates`;
    const ws = new WebSocket(wsUrl);

    ws.onmessage = (event) => {
        try {
            const message = JSON.parse(event.data);
            console.log('Received message:', message);

            if (message.type === 'graph' && message.elements) {
                updateGraph(message.elements);
            } else if (message.type === 'status' && message.nodeId && message.status) {
                updateNodeStatus(message.nodeId, message.status);
            }
        } catch (error) {
            console.error('Error processing message:', error);
            addStatusMessage('Error processing message', 'FAILED');
        }
    };

    ws.onerror = (error) => {
        console.error('WebSocket error:', error);
        addStatusMessage('WebSocket connection error', 'FAILED');
    };

    ws.onclose = () => {
        console.log('WebSocket disconnected. Reconnecting...');
        addStatusMessage('WebSocket disconnected, attempting to reconnect...', 'PENDING');
        setTimeout(initializeWebSocket, 2000);
    };

    ws.onopen = () => {
        addStatusMessage('Connected to WebSocket server', 'COMPLETED');
    };
}

function updateNodeStatus(nodeId, status) {
    if (!cy || !nodeId || !status || !STATUS_COLORS[status]) {
        console.error('Invalid update parameters:', { nodeId, status });
        return;
    }

    const node = cy.getElementById(nodeId);
    if (node.length === 0) {
        console.error('Node not found:', nodeId);
        return;
    }

    // Update node data and color
    node.data('status', status);
    node.style({
        'background-color': STATUS_COLORS[status]
    });

    // Update edge colors based on status
    if (status === 'RUNNING') {
        node.incomers('edge').style({
            'line-color': STATUS_COLORS.RUNNING,
            'target-arrow-color': STATUS_COLORS.RUNNING
        });
    } else if (status === 'COMPLETED') {
        node.outgoers('edge').style({
            'line-color': STATUS_COLORS.COMPLETED,
            'target-arrow-color': STATUS_COLORS.COMPLETED
        });
    } else if (status === 'FAILED') {
        node.connectedEdges().style({
            'line-color': STATUS_COLORS.FAILED,
            'target-arrow-color': STATUS_COLORS.FAILED
        });
    }

    addStatusMessage(`Node ${nodeId} status updated to ${status}`, status);
}

function addStatusMessage(message, status = 'PENDING') {
    const statusMessages = document.getElementById('status-messages');
    const messageElement = document.createElement('div');
    messageElement.className = `status-message ${status}`;

    const timestamp = document.createElement('div');
    timestamp.className = 'timestamp';
    timestamp.textContent = new Date().toLocaleTimeString();

    const content = document.createElement('div');
    content.textContent = message;

    messageElement.appendChild(timestamp);
    messageElement.appendChild(content);

    statusMessages.insertBefore(messageElement, statusMessages.firstChild);

    // Keep only last 50 messages
    while (statusMessages.children.length > 50) {
        statusMessages.removeChild(statusMessages.lastChild);
    }
}

function resetView() {
    if (cy) {
        cy.fit();
        cy.center();
        addStatusMessage('View reset to fit all nodes');
    }
}

function toggleTheme() {
    isDarkTheme = !isDarkTheme;
    document.documentElement.setAttribute('data-theme', isDarkTheme ? 'dark' : 'light');
    addStatusMessage(`Theme switched to ${isDarkTheme ? 'dark' : 'light'} mode`);
}