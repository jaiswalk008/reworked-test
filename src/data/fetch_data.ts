const csv = require('csv-parser');
import fs from "fs";
export const fetchCountyStateFromCSV = async () => {
    const results: any = [];
    const processedData: any = {};
    try {
        fs.createReadStream('uszips.csv')
            .pipe(csv())
            .on('data', (data: any) => results.push({ zip: data.zip, state_name: data.state_name, county_name: data.county_name, state_id: data.state_id }))
            .on('end', () => {
                // Assuming the CSV file has columns like zip, state_name, county_name, etc.
                // const processedData = {};

                results.forEach((entry: any) => {
                    const { zip, state_name, county_name, state_id } = entry;

                    if (county_name) {
                        let countyKey = `${county_name}, ${state_name} (county)`;
                        if (!processedData[countyKey]) {
                            processedData[countyKey] = {
                                type: 'county',
                                state_id,
                                zipcode: [],
                            };
                        }
                        processedData[countyKey].zipcode.push(zip);
                    }

                    if (state_name) {
                        let stateKey = `${state_name} (state)`; 
                        if (!processedData[stateKey]) {
                            processedData[stateKey] = {
                                type: 'state',
                                state_id,
                                zipcode: []
                            };
                        }
                        processedData[stateKey].zipcode.push(zip);
                    }
                });

                // console.log(JSON.stringify(processedData, null, 2));


                // let processedData: any = [];
                // results.forEach((entry: any) => {
                //     const { zip, state_name, county_name, state_id } = entry;

                //     let countyKey = `${county_name}, ${state_name} (county)`; // Adding "county" to the county key
                //     let existingEntry = processedData.find((item: any) => item.label === countyKey);
                //     if (!existingEntry && county_name) {
                //         existingEntry = {
                //             // type: 'county',
                //             state_id,
                //             label: countyKey,
                //             // zipcode: []
                //         };
                //         processedData.push(existingEntry);
                //     }

                //     // if (existingEntry) {
                //     //     existingEntry.zipcode.push(zip);
                //     // }
                //     let stateKey = `${state_name} (state)`; 
                //     existingEntry = processedData.find((item: any) => item.label === stateKey);
                //     if (!existingEntry && state_name) {
                //         existingEntry = {
                //             // type: 'state',
                //             state_id,
                //             label: stateKey,
                //             // zipcode: []
                //         };
                //         processedData.push(existingEntry);
                //     }

                //     // if (existingEntry) {
                //     //     existingEntry.zipcode.push(zip);
                //     // }
                // });


                // processedData = processedData.sort((a: any, b: any) => a.label.localeCompare(b.label));

                // sort the array
                console.log(processedData);

                // If you want to save the JSON to a file:
                fs.writeFileSync('outputjson.json', JSON.stringify(processedData, null, 2));
            });
    } catch (error) {
        console.error("errror in fetchCountyStateFromCSV")
        throw Error(error)
    }
}