const assert = require('assert');
const join = require('../joinStreams')
const Readable = require('stream').Readable

var comp = (a, b) => a.key < b.key ? -1 : a.key == b.key ? 0 : 1;



describe('joinStreams', function () {
	describe('inner', function () {
		it('should keep the records that have matching keys', function (done) {
			var result = [];
			join(createTestStream1(), createTestStream2(), comp, 'inner', fuse).on('data', function (data) {
				result.push(data)
			}).on('end', function () {
				assert.deepEqual(result,
					[{ key: 0, a: "A0", b: "B0" },
					{ key: 2, a: "A2", b: "B2" },
					{ key: 4, a: "A4", b: "B4" },
					{ key: 6, a: "A6", b: "B6" },
					{ key: 8, a: "A8", b: "B8" },
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
					{ key: 1, a: "A1", b: null },
					{ key: 2, a: "A2", b: "B2" },
					{ key: 3, a: "A3", b: null },
					{ key: 4, a: "A4", b: "B4" },
					{ key: 5, a: "A5", b: null },
					{ key: 6, a: "A6", b: "B6" },
					{ key: 7, a: "A7", b: null },
					{ key: 8, a: "A8", b: "B8" },
					{ key: 9, a: "A9", b: null },
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
					{ key: 2, a: "A2", b: "B2" },
					{ key: 4, a: "A4", b: "B4" },
					{ key: 6, a: "A6", b: "B6" },
					{ key: 8, a: "A8", b: "B8" },
					{ key: 10, a: null, b: "B10" },
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
					{ key: 1, a: "A1", b: null },
					{ key: 2, a: "A2", b: "B2" },
					{ key: 3, a: "A3", b: null },
					{ key: 4, a: "A4", b: "B4" },
					{ key: 5, a: "A5", b: null },
					{ key: 6, a: "A6", b: "B6" },
					{ key: 7, a: "A7", b: null },
					{ key: 8, a: "A8", b: "B8" },
					{ key: 9, a: "A9", b: null },
					{ key: 10, a: null, b: "B10" },
					])
				done()
			})
		})
	})
})



function createTestStream1() {
	var i = 0,
		stream = new Readable({ objectMode: true })
	stream._read = function () {
		if (i == 10) {
			this.push(null)
			i++;
		}

		while (i < 10 && this.push({ key: i, a: "A" + i }))
			i++
	}
	return stream
}


function createTestStream2() {
	var i = 0,
		stream = new Readable({ objectMode: true })
	stream._read = function () {
		if (i == 12) {
			this.push(null)
			i++
		}
		while (i < 12 && this.push({ key: i, b: "B" + i }))
			i += 2
	}
	return stream
}

function fuse(a, b) {
	var r = {};
	for (f in a)
		r[f] = a[f];
	for (f in b)
		if (!r[f] || b[f] || b[f] === 0) r[f] = b[f];
	return r;
}