const assert = require('assert');
const join = require('../joinStreams')
const Readable = require('stream').Readable
const repeatElement = require('repeat-element')
var comp = (a, b) => a.key < b.key ? -1 : a.key == b.key ? 0 : 1;



describe('joinStreams', function () {
	describe('simple inner', function () {
		it('should keep the records that have matching keys', function (done) {
			var result = [];
			join(createTestStream1(), createTestStream2(), comp, 'inner', fuse).on('data', function (data) {
				result.push(data)
			}).on('end', function () {
				assert.deepEqual(result,
					[{ key: 0, a: "A0", b: "B0" },
					{ key: 6, a: "A6", b: "B6" },
					{ key: 12, a: "A12", b: "B12" },
					])
				done()
			})
		})
	});

	describe('left', function () {
		it("should keep all the A's", function (done) {
			var result = [];
			join(createTestStream1(), createTestStream2(), comp, 'left', fuse).on('data', function (data) {
				result.push(data)
			}).on('end', function () {
				assert.deepEqual(result,
					[{ key: 0, a: "A0", b: "B0" },
					{ key: 3, a: "A3", b: null },
					{ key: 6, a: "A6", b: "B6" },
					{ key: 9, a: "A9", b: null },
					{ key: 12, a: "A12", b: "B12" },
					])
				done()
			})
		})
	});

	describe('right', function () {
		it("should keep all the B's", function (done) {
			var result = [];
			join(createTestStream1(), createTestStream2(), comp, 'right', fuse).on('data', function (data) {
				result.push(data)
			}).on('end', function () {
				assert.deepEqual(result,
					[{ key: 0, a: "A0", b: "B0" },
					{ key: 2, a: null, b: "B2" },
					{ key: 4, a: null, b: "B4" },
					{ key: 6, a: "A6", b: "B6" },
					{ key: 8, a: null, b: "B8" },
					{ key: 10, a: null, b: "B10" },
					{ key: 12, a: "A12", b: "B12" },
					])
				done()
			})
		})
	})

	describe('outer', function () {
		it("should keep both A's and B's", function (done) {
			var result = [];
			join(createTestStream1(), createTestStream2(), comp, 'outer', fuse).on('data', function (data) {
				result.push(data)
			}).on('end', function () {
				assert.deepEqual(result,
					[{ key: 0, a: "A0", b: "B0" },
					{ key: 2, a: null, b: "B2" },
					{ key: 3, a: "A3", b: null },
					{ key: 4, a: null, b: "B4" },
					{ key: 6, a: "A6", b: "B6" },
					{ key: 8, a: null, b: "B8" },
					{ key: 9, a: "A9", b: null },
					{ key: 10, a: null, b: "B10" },
					{ key: 12, a: "A12", b: "B12" },
					])
				done()
			})
		})
	})
	describe('multiplicity,inner', function () {
		it("should match every instance of a key with all instances of same key from other stream", function (done) {
			var result = [];
			join(createTestStream3(), createTestStream4(), comp, 'inner', fuse).on('data', function (data) {
				result.push(data)
			}).on('end', function () {
				assert.deepEqual(result, [
					{ key: 0, a: "A0", b: "B0" },
					{ key: 0, a: "A0", b: "B1" },
					{ key: 0, a: "A0", b: "B2" },
					{ key: 1, a: "A1", b: "B3" },
					{ key: 1, a: "A2", b: "B3" },
					{ key: 1, a: "A3", b: "B3" },
					{ key: 2, a: "A4", b: "B4" },
					{ key: 4, a: "A6", b: "B5" },
					{ key: 4, a: "A6", b: "B6" },
					{ key: 4, a: "A6", b: "B7" },
					{ key: 4, a: "A7", b: "B5" },
					{ key: 4, a: "A7", b: "B6" },
					{ key: 4, a: "A7", b: "B7" },
					{ key: 4, a: "A8", b: "B5" },
					{ key: 4, a: "A8", b: "B6" },
					{ key: 4, a: "A8", b: "B7" },
				])
				done()
			})
		})
	})
})



	function createTestStream1() {
		return createArithmaticStream("A", "a", 3, 12);
	}

	function createTestStream2() {
		return createArithmaticStream("B", "b", 2, 12);
	}


	function createArithmaticStream(char, field, step, last) {
		var i = 0,
			stream = new Readable({ objectMode: true }),
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
	}

	function createRepeatingKeyStream(keysAndMultiplicity, char, field) {
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
	}




	function createTestStream3() {
		return createRepeatingKeyStream({
			0: 1,
			1: 3, // 1-3
			2: 1, // 4
			3: 1,  // 5
			4: 3  // 6-8 
		}, "A", "a")
	}

	function createTestStream4() {
		return createRepeatingKeyStream({
			0: 3, // 0 -2
			1: 1, // 3
			2: 1,  // 4

			4: 3  // 5-7
		}, "B", "b")
	}


	/*
	function createTestStream3() {
		var k = [0, 0, 0, 1, 1, 1, 2, 2, 2, 2],
			m = k.map((_, ind) => ind),
			stream = new Readable({ objectMode: true })
		stream._read = function () {
			do {
				var obj = { key: k.shift(), a: "A" + m.shift() }
			} while (k.length && this.push(obj))
			if (!k.length)
				this.push(null)
		}
	}
	*/
	function fuse(a, b) {
		var r = {};
		for (f in a)
			r[f] = a[f];
		for (f in b)
			if (!r[f] || b[f] || b[f] === 0) r[f] = b[f];
		return r;
	}