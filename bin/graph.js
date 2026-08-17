const fs = require('fs');
var graph = JSON.parse(fs.readFileSync('graph.json').toString());
const edges = [], nodes = [];

function nodeClasses(step) {
    return step.kind == 'node' : 'node' ? step.parallel ? 'sync-parallel' : 'sync-serial'
}

function nodeParallel(step, previous = []) {
    const next = []
    for (const child of step.children) {
        let id = `${child.under}-${child.name}`
        nodes.push({ id: id, classes: 'sync-parallel' })
        for (const source of previous) {
            edges.push({ source: source, target: id })
        }
        next.push.apply(next, nodeSteps(child.children, [ id ]))
    }
    return next
}

function nodeSteps (steps, previous = []) {
    let id, parallelId
    for (const step of steps) {
        id = `${step.under}-${step.name}`
        if (previous.length != 0) {
            for (const source of previous) {
                edges.push({ source: source, target: id })
            }
        }
        if (step.children.length != 0) {
            if (step.parallel) {
                previous = nodeParallel(step, [ id ])
            } else {
                for (const child of step.children) {
                    previous.push.apply(previous, nodeSteps(step.children, id))
                }
            }
        } else {
            previous = [ id ]
        }
    }
    return previous
}

function nodeChildren () {
}

nodeSteps(graph);
console.log(nodes);
