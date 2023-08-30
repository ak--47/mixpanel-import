//@ts-nocheck
import streamType from './streamTypes.mjs';
import worker from './workers.mjs';
import chunkVsMem from './streamsVsMemory.mjs';
import profiler from './profiler.mjs';

// const JSONvsNDJSONvsStreams = await streamType();
// const oneToTwoHundredWorkers = await worker();
const foo = await profiler();
console.log(foo);
debugger