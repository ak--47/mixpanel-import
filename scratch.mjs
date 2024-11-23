import dm4 from 'make-mp-data';
import main from './index.js';
import { comma } from 'ak-tools';

const token = "25ed954dfcc44911985e9005733fe49a";
const secret = "2071a481b6f0de8300e7d92911cfc687";
const project = 3484336


const data = await dm4({
	token,
	seed: "futz",
	numUsers: 10_000,
	numEvents: 50_000,
	groupKeys: [["company_id", 2500]],
	groupProps: {
		company_id: {
			"dude": ["man", "bro", "guy"]
		}
	}
});

const profiles = await main({ secret, project, token }, {}, { recordType: "profile-delete", skipWriteToDisk: true })

debugger;

