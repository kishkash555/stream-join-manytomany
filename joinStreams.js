const fs = require('fs');
const readable = require('stream').Readable;

function joinStreams(streamA, streamB, comp, joinType, fuse) {
    // streamA and streamB must already be sorted
    var joinReadable = new readable({ objectMode: true });

    var keepAs = false,
        keepBs = false,
        isDoneA = false,
        isDoneB = false,
        objA,
        objB,
        emptyA,
        emptyB,
        sinkReady;

    var Ai = 0, Bi = 0;

    switch (joinType.toLowerCase()) {
        case 'left':
            keepAs = true;
            break;
        case 'right':
            keepBs = true;
            break;
        case 'outer':
        case 'full-outer':
        case 'full_outer':
        case 'fullouter':
            keepAs = true;
            keepBs = true;
            break;
    }

    streamA.on('data', function (data) {
        if (objA) {
            throw new Error('Stream A fired unexpectedly');
        }
        objA = data;
        streamA.pause();

        emptyA = emptyA || nullAllFields(objA);
        if (objB || isDoneB)
            joinLoop();
    });

    streamB.on('data', function (data) {
        if (objB) {
            throw new Error('Stream A fired unexpectedly');
        }
        objB = data;
        streamB.pause();

        emptyB = emptyB || nullAllFields(objB);
        if (objA || isDoneA)
            joinLoop();
    });

    streamA.on('end', function () {
        isDoneA = true;
        if (objB || isDoneB)
            joinLoop();
    })

    streamB.on('end', function () {
        isDoneB = true;
        if (objA || isDoneA)
            joinLoop();
    })

    streamA.pause();
    streamB.pause();

    joinReadable._read = function () {
        if (!sinkReady) {
            sinkReady = true;
            if (!(isDoneA || objA)) {
                streamA.resume();
            }
            if (!(isDoneB || objB)) {
                streamB.resume();
            }
        }
    }
    return joinReadable;



    function joinLoop() {
        var toPush;
        if ((objA || isDoneA) && (objB || isDoneB)) {
            if (isDoneA || isDoneB) {
                toPush = finishLoop(this)
                objA = undefined;
                objB = undefined;
            }
            else
                switch (comp(objA, objB)) {
                    case -1: // A < B
                        if (keepAs)
                            toPush = fuse(objA, emptyB);
                        objA = undefined;
                        break;
                    case 0: // A matches B
                        toPush = fuse(objA, objB);
                        objA = undefined;
                        objB = undefined;
                        break;
                    case 1: // A > B
                        if (keepBs)
                            toPush = fuse(emptyA, objB);
                        objB = undefined;
                        break;
                }
            if (typeof toPush != 'undefined')
                sinkReady = joinReadable.push(toPush);
            if (sinkReady) {
                if (!(isDoneA || objA)) {
                    streamA.resume();
                }
                if (!(isDoneB || objB)) {
                    streamB.resume();
                }
            }
        }
        else
            throw new Error('joinLoop called with objA: ' + JSON.stringify(objA) + ', objB: ' + JSON.stringify(objB))
    }

    function finishLoop() {
        var toPush;
        if (isDoneB && !isDoneA && keepAs && objA) { // B stream depleted
            toPush = fuse(objA, emptyB);
        }
        else if (isDoneA && !isDoneB && keepBs && objB) { // A stream depleted
            toPush = fuse(emptyA, objB);
        }
        if (isDoneA && isDoneB) {
            console.log('joinStreams done')
            toPush = null;
        }
        return toPush;
    }
}

function nullAllFields(obj) {
    var res = {};
    Object.keys(obj).forEach(function (k) {
        res[k] = null;
    })
    return res;
}



module.exports = joinStreams;

