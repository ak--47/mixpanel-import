/* eslint-disable no-unused-vars */
// @ts-check
const foo = require('../index.js');


foo({}, '', {transformFunc: (a) => { return a}}, true);

foo.createMpStream({}, {})
