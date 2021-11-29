import fs from 'fs';
import readline from 'node:readline'
import { MongoClient } from 'mongodb'

export const parseReadCSV = async () => {
	//console.time("timer"); //Timer to know the time to read and create CSV
	const data = fs.createReadStream('StockEtablissement_utf8.csv');

	const rl = readline.createInterface({
		input: data
	})

	var i = 0; //index
	var totalLength = 0; //Total Length of all the files 
	var buffer = []; //Buffer to stock the line
	for await (const line of rl) {
		buffer += line + '\n';
		totalLength += line.length;
		if (buffer.length >= 60000000) { 
			//console.log(totalLength);
			createFile(buffer, i)
			i++;
			buffer = [];
		}
	}
	createFile(buffer, i);
	//console.timeEnd("timer");
}

//Create the CSV FILE with current data in it
const createFile = (data, name) => {
	fs.writeFile('./tmp/test' + name + '.csv', data, 'utf8', function (err) {
		if (err) {
			console.log('ERROR');
		} else {
			console.log('It\'s saved!');
		}
	});
}

const ParseLittleFile = async (filename) => {
	const data = fs.createReadStream(`${filename}`);
	const rl = readline.createInterface({
		input: data
	})
	var buffer = [];
	for await (const line of rl) {
		const TmpSplit = line.split(',');
		const finalSplit = {
			siren: TmpSplit[0], nic: TmpSplit[1], siret: TmpSplit[2], dateCreationEtablissement: TmpSplit[4], dateDernierTraitementEtablissement: TmpSplit[8]
			, typeVoieEtablissement: TmpSplit[14], libelleVoieEtablissement: TmpSplit[15], codePostalEtablissement: TmpSplit[16],
			libelleCommuneEtablissement: TmpSplit[17], codeCommuneEtablissement: TmpSplit[20], libellePaysEtranger2Etablissement: TmpSplit[38],
			dateDebut: TmpSplit[39], etatAdministratifEtablissement: TmpSplit[40]
		};;
		var propertyList = Object.keys(finalSplit);
		propertyList.forEach(currentElement => {
			if (finalSplit[currentElement] == '') {
				delete finalSplit[currentElement];
			}
		})
		buffer.push(finalSplit);
	}
	return buffer;
}



const sendData = async (filename) => {
	console.time("timerSendData");
	const url = 'mongodb://localhost:27017';
	const dbName = 'DabeTest';
	let mydb;
	let client
	const myObject = await ParseLittleFile(`tmp/test${filename}.csv`);
	try {
		client = await MongoClient.connect(url, {});
		mydb = client.db(dbName);
		var myCol = mydb.collection("DabeTest");
		var bulk = myCol.initializeUnorderedBulkOp();
		for (let i = 0; myObject[i] !== undefined; i++) {
			bulk.insert(myObject[i]);
		}
		await bulk.execute();
	}
	finally {
		if (client) {
			client.close();
		}
	}
	console.timeEnd('timerSendData');
};

await sendData(0);
await sendData(93);
const first = parseInt(process.argv[2], 10);
const last = parseInt(process.argv[3], 10);
for (let i = first; i <= last; i++) {
	console.log(i);
	await sendData(i);
}

/*const bufferFolder = [];
fs.readdirSync('./tmp').forEach(file => {
bufferFolder.push(file);
});*/


//sendData('tmp/test1.csv');
//sendData('tmp/test2.csv');


//console.log(process.argv[2]);

//sendData(process.argv[2]);