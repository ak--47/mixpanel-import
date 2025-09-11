
/**
 * @fileoverview
 * this file allows you to manipulate metadata about the project
 */

const dayjs = require('dayjs');
const utc = require('dayjs/plugin/utc');
dayjs.extend(utc);
const got = require('got');
const u = require('ak-tools');



/** @typedef {import('./job.js')} Job */



const common = {
	"Accept": "application/json",
	"Origin": "https://mixpanel.com"
};

/**
 * @param  {Array<{time: string, message: string, [key: string]: string}>} annotations
 * @param  {Job} job
 */
async function replaceAnnotations(annotations, job) {
	const deleted = await deleteAnnotations(job);
	const modeledAnnotations = annotations.map(a => {
		const annotation = { date: "", description: "generic description" };
		let annotationTime;
		let annotationDescription;
		if (a.time) annotationTime = dayjs(a.time).utc().format('YYYY-MM-DD HH:mm:ss');
		if (a.date) annotationTime = dayjs(a.date).utc().format('YYYY-MM-DD HH:mm:ss');
		if (a.message) annotationDescription = a.message;
		if (a.description) annotationDescription = a.description;
		if (!annotationTime || !annotationDescription) throw new Error("missing time or date");
		annotation.date = annotationTime;
		annotation.description = annotationDescription;
		return annotation;
	});
	const results = [];
	for (const annotation of modeledAnnotations) {
		const res = await createAnnotation(annotation, job);
		results.push(res);
	}
	// @ts-ignore
	job.dryRunResults = { created: results, deleted };
	return { created: results, deleted };

}


/**
 * @param  {Job} job
 */
async function getAnnotations(job) {
	const { project = "", bearer = "" } = job.creds;
	if (!project || !bearer) throw new Error("missing project or bearer");
	const url = `https://mixpanel.com/api/app/projects/${project}/annotations`;
	let req, res;
	try {
		// @ts-ignore
		req = await got(url, { ...common, headers: { "Authorization": `Bearer ${bearer}`, method: "GET" } });
		res = JSON.parse(req.body);
		job.dryRunResults = res?.results || [];
		return res;

	}
	catch (e) {
		return handleError(e);
	}
}

/**
 * @param  {{date: string, description: string}} payload
 * @param  {Job} job
 */
async function createAnnotation(payload, job) {
	const { project = "", bearer = "" } = job.creds;
	if (!project || !bearer) throw new Error("missing project or bearer");
	const url = `https://mixpanel.com/api/app/projects/${project}/annotations`;
	let req, res;
	try {
		// @ts-ignore
		req = await got.post(url, { headers: { ...common, "Authorization": `Bearer ${bearer}`, "Content-Type": "application/json" }, body: JSON.stringify(payload) });
		res = JSON.parse(req.body);
		return res;

	}

	catch (e) {
		return handleError(e);
	}
}

/**
 * @param  {Job} job
 */
async function deleteAnnotations(job) {
	const { project = "", bearer = "" } = job.creds;
	if (!project || !bearer) throw new Error("missing project or bearer ");
	const annotations = await getAnnotations(job);
	const { results = [] } = annotations;
	const responses = [];
	for (const annotation of results) {
		const url = `https://mixpanel.com/api/app/projects/${project}/annotations/${annotation.id}`;

		try {
			// @ts-ignore
			// const auth = `Basic ${Buffer.from(acct + ':' + secret, 'binary').toString('base64')}`;
			const req = await got.delete(url, { headers: { ...common, "Authorization": `Bearer ${bearer}`, method: "DELETE" } });
			const res = JSON.parse(req.body);
			responses.push(res);

		}
		catch (e) {
			return handleError(e);
		}
	}
	job?.dryRunResults?.push(...responses);
	return responses;
}

function handleError(e) {
	if (u.isJSONStr(e?.response?.body)) {
		return JSON.parse(e.response.body);
	}
	else {
		return e;
	}
}



module.exports = {
	replaceAnnotations,
	getAnnotations,
	deleteAnnotations
};