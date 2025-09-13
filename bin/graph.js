console.log('hello')

const fs = require('fs')
var graph = JSON.parse(fs.readFileSync('graph.json').toString())

function nodeSteps (steps, prevoiusId = null) {
    for (const step of steps) {
        console.log(step)
    }
}

function nodeChildren () {
}

nodeSteps(graph)
