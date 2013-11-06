var
	path = require("path"),
	fs = require("fs")
;

var testFiles = fs.readdirSync("./node_modules/http-cache/test");

for (var i = 0; i < testFiles.length; i++) {

	var file = testFiles[i];
	if (file === "setup.js")
		continue; // ignore this one file

	var fileData = fs.readFileSync("./node_modules/http-cache/test/" + file);
	fs.writeFileSync("./test/" + file, fileData);
}
