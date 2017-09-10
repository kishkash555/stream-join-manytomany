"use strict"
const assert = require('assert');
const join = require('../joinStreams')
const testTools = require('../testTools.js')

var comp = (a, b) => a.key < b.key ? -1 : a.key == b.key ? 0 : 1;

// ********** tests ********** ////////

describe('joinStreams', function () {
	describe('simple inner', function () {
		it('should keep the records that have matching keys', function (done) {
			var result = [];
			var a = join(createTestStream1(), createTestStream2(), { comp, joinType: 'inner' })
				.on('data', function (data) {
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
			join(createTestStream1(), createTestStream2(), { comp, joinType: 'left' }).on('data', function (data) {
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
			join(createTestStream1(), createTestStream2(), { comp, joinType: 'right' }).on('data', function (data) {
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
			join(createTestStream1(), createTestStream2(), { comp, joinType: 'outer' }).on('data', function (data) {
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
	describe('multiplicity (inner)', function () {
		it("should match every instance of a key with all instances of same key from other stream", function (done) {
			var result = [];
			join(createTestStream3(), createTestStream4(), { comp, joinType: 'inner' }).on('data', function (data) {
				result.push(data)
			}).on('end', function () {
				assert.deepEqual(result, [
					{ key: "0", a: "A0", b: "B0" },
					{ key: "0", a: "A0", b: "B1" },
					{ key: "0", a: "A0", b: "B2" },
					{ key: "1", a: "A1", b: "B3" },
					{ key: "1", a: "A2", b: "B3" },
					{ key: "1", a: "A3", b: "B3" },
					{ key: "2", a: "A4", b: "B4" },
					{ key: "4", a: "A6", b: "B5" },
					{ key: "4", a: "A6", b: "B6" },
					{ key: "4", a: "A6", b: "B7" },
					{ key: "4", a: "A7", b: "B5" },
					{ key: "4", a: "A7", b: "B6" },
					{ key: "4", a: "A7", b: "B7" },
					{ key: "4", a: "A8", b: "B5" },
					{ key: "4", a: "A8", b: "B6" },
					{ key: "4", a: "A8", b: "B7" },
				])
				done()
			})
		})
	})

	describe('multiplicity with slow sink (inner)', function () {
		it("should produce the same results as with fast sink", function (done) {
			var result = [];
			var a = join(createTestStream3(), createTestStream4(), { comp, joinType: 'inner', highWaterMark: 2 })
			a.pipe(testTools.slowWriter())
			a.on('data', function (data) {
				result.push(data)
			}).on('end', function () {
				assert.deepEqual(result, [
					{ key: "0", a: "A0", b: "B0" },
					{ key: "0", a: "A0", b: "B1" },
					{ key: "0", a: "A0", b: "B2" },
					{ key: "1", a: "A1", b: "B3" },
					{ key: "1", a: "A2", b: "B3" },
					{ key: "1", a: "A3", b: "B3" },
					{ key: "2", a: "A4", b: "B4" },
					{ key: "4", a: "A6", b: "B5" },
					{ key: "4", a: "A6", b: "B6" },
					{ key: "4", a: "A6", b: "B7" },
					{ key: "4", a: "A7", b: "B5" },
					{ key: "4", a: "A7", b: "B6" },
					{ key: "4", a: "A7", b: "B7" },
					{ key: "4", a: "A8", b: "B5" },
					{ key: "4", a: "A8", b: "B6" },
					{ key: "4", a: "A8", b: "B7" },
				])
				done()
			})
		})
	})

})

// ********** helper functions ********** ////////

function createTestStream1() {
	return testTools.createArithmaticStream("A", "a", 3, 12);
}

function createTestStream2() {
	return testTools.createArithmaticStream("B", "b", 2, 12);
}



function createTestStream3() {
	return testTools.createRepeatingKeyStream({
		"0": 1,
		"1": 3, // 1-3
		"2": 1, // 4
		"3": 1,  // 5
		"4": 3  // 6-8 
	}, "A", "a")
}

function createTestStream4() {
	return testTools.createRepeatingKeyStream({
		"0": 3, // 0 -2
		"1": 1, // 3
		"2": 1,  // 4

		"4": 3  // 5-7
	}, "B", "b")
}



