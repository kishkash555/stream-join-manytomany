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
            console.log('streamA data fired unexpectedly')
            Ai++;
        }
        objA = data;
        //console.log('objA: ' + JSON.stringify(objA))
        streamA.pause();

        emptyA = emptyA || nullAllFields(objA);
        if (objB || isDoneB)
            joinLoop();
    });

    streamB.on('data', function (data) {
        if (objB) {
            console.log('streamB data fired unexpectedly')
            Bi++;
        }
        objB = data;
        //console.log('pausing B')
        streamB.pause();

        emptyB = emptyB || nullAllFields(objB);
        if (objA || isDoneA)
            joinLoop();
    });
    streamA.on('end', function () {
        //console.log('streamA end')
        isDoneA = true;
        if (objB || isDoneB)
            joinLoop();
    })
    streamB.on('end', function () {
        //console.log('streamB end')
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
                //console.log('resuming A');
                streamA.resume();
            }
            if (!(isDoneB || objB)) {
                //console.log('resuming B')
                streamB.resume();
            }
        }
    }
    return joinReadable;



    function joinLoop() {
        var toPush;
        if ((objA || isDoneA) && (objB || isDoneB)) {
            //console.log(`objA: ${JSON.stringify(objA)}, comp: ${comp(objA, objB)}`)
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
                    //console.log('resuming A');
                    streamA.resume();
                }
                if (!(isDoneB || objB)) {
                    //console.log('resuming B')
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
    var res = {}, objKeys = Object.keys(obj)
    for (k of objKeys) res[k] = null;
    return res;
}
/*
test1 = function () {
    var emptyA = { id: 0, account: "", domain: "" };
    var emptyB = { id: 0, account: "", manager: "" };

    var streamA = csv2array.CSVfileReader('sampleInput1.csv', emptyA);
    var streamB = csv2array.CSVfileReader('sampleInput2.csv', emptyB);
    var outStream = fs.createWriteStream('sampleOutput.csv');

    streamA.on('close', function () { console.log('Stream A closed!') })
    streamB.on('close', function () { console.log('Stream B closed!') })

    comp = function (a, b) { return a.account < b.account ? -1 : a.account == b.account ? 0 : 1; }
    fuse = function (a, b) { var a = csvjson.toCSV({ id: a.id, account: a.account, domain: a.domain, manager: b.manager }, { headers: "none" }); console.log(a); return (a); };

    joinStreams(streamA, streamB, comp, 'right', fuse, emptyA, emptyB).pipe(outStream);

}
*/
module.exports = joinStreams;

