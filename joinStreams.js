"use strict"
const fs = require('fs');
const readable = require('stream').Readable;

function joinStreams(streamA, streamB, options) {
    // streamA and streamB must already be sorted.
    // if they aren't, there won't be an error
    // but the join may not work as expected
    options = Object.assign(defaultOptions(), options);
    var joinReadable = new readable({ objectMode: true, highWaterMark: options.highWaterMark || 16 }),
        comp = options.comp,
        joinType = options.joinType,
        fuse = options.fuse;

    var keepAs = false,
        keepBs = false,
        isDoneA = false,
        isDoneB = false,
        objA,
        objB,
        emptyA,
        emptyB,
        sinkReady,
        aMatchPool = [],
        bMatchPool = [],
        ai = 0,
        bi = 0,
        matchPoolOpen = false,
        pausedWritingMatchPool = false;

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
        if (objB)
            handleNewAandB();
        if (isDoneB)
            handleNewAorB()
    });

    streamB.on('data', function (data) {
        if (objB) {
            throw new Error('Stream A fired unexpectedly');
        }
        objB = data;
        streamB.pause();

        emptyB = emptyB || nullAllFields(objB);
        if (objA)
            handleNewAandB();
        if (isDoneA)
            handleNewAorB();
    });

    streamA.on('end', function () {
        isDoneA = true;
        if (objB || isDoneB)
            handleNewAorB();
    })

    streamB.on('end', function () {
        isDoneB = true;
        if (objA || isDoneA)
            handleNewAorB();
    })

    streamA.pause();
    streamB.pause();

    joinReadable._read = function () {
        sinkReady = true;
        writeOutputOrResumeInputs();
    }

    function writeOutputOrResumeInputs() {
        if (pausedWritingMatchPool) // 
            writeMatchPool()
        else {
            if (objA && isDoneB || isDoneA && objB)
                handleNewAorB();
            else if (objA && objB)
                handleNewAandB();
            else {
                if (!(isDoneA || objA)) {
                    streamA.resume();
                }
                if (!(isDoneB || objB)) {
                    streamB.resume();
                }
            }
        }
    }

    return joinReadable;

    function handleNewAandB() {
        if (matchPoolOpen) {
            // console.log('matchPoolOpen')
            matchPoolOpen = false;
            if (!isDoneB && comp(aMatchPool[0], objB) == 0) {
                // console.log('objB ' + JSON.stringify(objB) + ' matched')
                bMatchPool.push(objB);
                objB = undefined;
                matchPoolOpen = true;
                streamB.resume();
            }
            if (!isDoneA && comp(objA, bMatchPool[0]) == 0) {
                // console.log('objA ' + JSON.stringify(objB) + ' matched')
                aMatchPool.push(objA);
                objA = undefined;
                matchPoolOpen = true;
                streamA.resume();
            }
        }
        if (!matchPoolOpen) {
            writeMatchPool()
            if (pausedWritingMatchPool) // sink is still buffering. wait for a read event;
                return;
            // console.log('compare ' + JSON.stringify(objA) + ' and ' + JSON.stringify(objB))
            switch (comp(objA, objB)) {
                case -1: // A < B
                    if (keepAs) {
                        aMatchPool.push(objA)
                        bMatchPool.push(emptyB)
                        pausedWritingMatchPool = true;
                    }
                    objA = undefined;
                    break;
                case 0: // A matches B
                    aMatchPool.push(objA)
                    bMatchPool.push(objB)
                    objA = undefined;
                    objB = undefined;
                    matchPoolOpen = true;
                    break;
                case 1: // A > B
                    if (keepBs) {
                        aMatchPool.push(emptyA)
                        bMatchPool.push(objB)
                        pausedWritingMatchPool = true;
                    }
                    objB = undefined;
                    break;
            }
            writeOutputOrResumeInputs();
        }
    }

    function writeMatchPool() {
        // console.log('wirteMatchPool')
        pausedWritingMatchPool = false; // prevent this function from being called twice
        var al = aMatchPool.length,
            bl = bMatchPool.length;
        if (!(al && bl)) return

        for (; ai < al && sinkReady; ai++) {
            for (; bi < bl && sinkReady; bi++) {
                sinkReady = joinReadable.push(fuse(aMatchPool[ai], bMatchPool[bi]))
                // console.log('push')
            }
            if (bi == bl) bi = 0;
        }
        if (sinkReady) {
            ai = 0;
            aMatchPool = [];
            bMatchPool = [];
            writeOutputOrResumeInputs();
        }
        else {
            pausedWritingMatchPool = true;
            console.log('pausedWritingMatchPool')
        }
    }


    function handleNewAorB() {

        var toPush;
        if (isDoneB && !isDoneA && keepAs && objA) { // B stream depleted
            toPush = fuse(objA, emptyB);
        }
        else if (isDoneA && !isDoneB && keepBs && objB) { // A stream depleted
            toPush = fuse(emptyA, objB);
        }
        else if (isDoneA && isDoneB) {
            writeMatchPool()
            // console.log('joinStreams done')
            if (!pausedWritingMatchPool)
                toPush = null;
        }
        if (!(typeof toPush == 'undefined'))
            sinkReady = joinReadable.push(toPush);
    }
}

function nullAllFields(obj) {
    var res = {};
    Object.keys(obj).forEach(function (k) {
        res[k] = null;
    })
    return res;
}

function defaultOptions() {
    return {

        fuse: (a, b) => {
            var r = {};
            for (var f of Object.keys(a))
                r[f] = a[f];
            for (f of Object.keys(b))
                if (!r[f] || b[f] || b[f] === 0) r[f] = b[f];
            return r;
        },
        joinType: 'inner',

    }
}

module.exports = joinStreams;

