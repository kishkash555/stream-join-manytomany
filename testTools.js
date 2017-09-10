"use strict"
const Stream = require('stream')
const Readable = Stream.Readable
const Writable = Stream.Writable
const time = require('time-since')
const start = new Date();

module.exports = {
    createArithmaticStream: function (char, field, step, last) {
        var i = 0,
            stream = new Readable({ objectMode: true, highWaterMark: 2 }),
            p
        stream._read = function () {
            if (i == last + step) {
                this.push(null)
                i++
            }
            else do {
                var j = { key: i };
                j[field] = char + i;
                p = this.push(j)
                i += step
            }
            while (i <= last && p)
        }
        return stream
    },

    createRepeatingKeyStream: function (keysAndMultiplicity, char, field) {
        var ind = 0,
            keys = Object.keys(keysAndMultiplicity),
            stream = new Readable({ objectMode: true }),
            done = false;
        stream._read = function () {
            do {
                var obj = { key: keys[0] };
                obj[field] = char + ind;
                ind++;
                var p = this.push(obj)
                if (!--keysAndMultiplicity[keys[0]]) // decrement the multiplicity counter and check if zero
                    keys.shift() //remove keys[0] from keys
            } while (p && keys.length)
            if (!keys.length & !done) {
                done = true;
                this.push(null)
            }
        }
        return stream;
    },

    slowWriter: function () {
        var ret = new Writable({ objectMode: true, highWaterMark: 2 })
        ret._write = function (data, _, callback) {
            setTimeout(callback, 75)
        }
        return ret
    },


    trivialTransform: function () {
        var ret = new Stream.Transform({ objectMode: true, highWaterMark: 2 })
        ret._transform = function (data, _, callback) {
            var p = this.push(data);
            if (!p)
                console.log('buffered')
            callback();
        }
        return ret
    },
}