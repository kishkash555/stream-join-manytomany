Join Streams
============

Joins the fields from two object streams, to create a single object stream in the output.
Unlike other stream-joining modules available on NPM, this module combines objects from the first stream with objects from the second stream that have certain fields (designated as keys) whose values match. In other words, this is an implementation of an SQL JOIN for [Node JS streams](https://nodejs.org/api/stream.html), including support for INNER, LEFT and OUTER joins.

 
# Installation
```
$ npm install --save join_streams
```

# Example
```js
const comparer = require('flat-object-compare');
const join = require('join_streams')
const streamify = require('stream-array')

var comp = comparer.objectComparison(['country',]);

var stream1 = [
    { country: 'Mexico', capital: 'Mexico City' },
    { coutnry: 'Indonesia', capital: 'Jakarta' },
    { country: 'Nigeria', capital: 'Abuja'},
    { country: 'Turkey', capital: 'Ankara'},
]

var stream2 = [
    { country: 'Mexico', gdp: 1283 },
    { coutnry: 'Indonesia', gdp: 2686 },
    { country: 'Nigeria', gdp: 574},
    { country: 'Turkey', gdp: 1508},    
]

var result =[];

join(streamify(stream1.sort(comp)), streamify(stream2.sort(comp)), { comp: comp, joinType: 'inner' })
				.on('data', function (data) {
					result.push(data)
				}).on('end', function () {
                    console.log(JSON.stringify(result))
                })
/*
For inner join, only the countries that appear in both streams (i.e. comparison function returns a zero) will appear in output.
[{"coutnry":"Indonesia","capital":"Jakarta","gdp":889},{"country":"Mexico","capital":"Mexico City","gdp":1283},{"country":"Nigeria","capital":"Abuja","gdp":574},{"country":"Turkey","capital":"Ankara","gdp":751}]
*/
```

# API
`join(streamA, streamB, options)`

*streamA*, *streamB* - streams to join. 

*options* - an object with the following fields:
* **comp**: a comparison function, which takes two parameters (objects of streamA and streamB respectively) and returns -1, 0, or 1 when objectA's key comes before, is equal to, or comes after objectB's key (respectively). This field is mandatory. See [flat-object-compare](https://www.npmjs.com/package/flat-object-compare) npm module for a quick way to generate a comparison function from just the field names.
* **joinType**:
    * 'inner' - performs inner join. Output stream will contain only objects where the key matched between streamA and streamB.
    * 'left' - non-matched objects from streamA are preserved; non-matched objects from streamB are discarded
    * 'outer' - non-matched objects from both streamA and streamB are preserved in the output.
* **fuse**: an optional field. This controls how the output object will be created from the two input objects. Overriding the default is discouraged except when very specific behavior is desired with regards to empty values (`NaN`,`undefined`). To override default, supply a function that takes in two object arguments and return an object. join_streams calls the fuse function on any pair of matching objects, and sends to the output the object returned by the **fuse** function. 

## Notes 

1. with 'left' or 'outer' join, non-matched objects are passed to the output stream. These objects will still contain all the fields of both A and B; the missing fields will have the value `null`. To change this behavior, provide an implementation for the fuse function.
1. the default join type is 'inner'.
1. `join_streams` assumes each input stream was sorted by the same comparison function that is used to join. An unsorted stream will not raise any exception, but the result may be incomplete.
1. `join_streams` fully supports repeating keys and outputs all the corresponding matches.
2. `join_streams` is very lean on memory. Its memory consumption does not rise with the number of processed elements. Therefore it is suitable for prototyping big-data pipelines.


