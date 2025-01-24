

export const leadgenModel: any = {
    // "real_estate_investors" : {
    //     api_url : "https://list.melissadata.net/v1/Consumer/rest/Service.svc",
    //     api_key : 120506494,
    //     criteria: {
    //         dwell: 2,
    //         ownRent: 1,
    //         'cAge-d': "7-8-9-10-11",
    //         hInc: "7-8-9-10-11-12-13"
    //     },
    // },
    "roofing" : {
        api_url : "http://list.melissadata.net/v2/Property",
        api_key : 120506494,
        criteria: {
            occu: '1', // Occupancy: 1 (Owner occupied), 2 (Absentee owner - all types), 3 (Absentee owner - In County),
               // 4 (Absentee owner - In State), 5 (Absentee owner - Out of Country), 6 (Absentee owner - Out of State)
            hhage: '5-6', // Age ranges: 1 (18-24), 2 (25-34), 3 (35-44), 4 (45-54), 5 (55-64), 6 (65-74), 7 (75+)
            hhinc: 'C-D-E-F', // Income ranges: 1 (<$10,000), 2 ($10,000-14,999), 3 ($15,000-19,999), 4 ($20,000-24,999),
                // 5 ($25,000-29,999), 6 ($30,000-34,999), 7 ($35,000-39,999), 8 ($40,000-44,999),
                // 9 ($45,000-49,999), A ($50,000-54,999), B ($55,000-59,999), C ($60,000-64,999),
                // D ($65,000-69,999), E ($70,000-99,999), F ($100,000-149,999), G ($150,000-174,999),
                // H ($175,000-199,999), I ($200,000-249,999), J ($250,000+)
            people: '1-2-3', // Persons in household: 1, 2, 3, 4, 5, 6, 7, 8, 9, 10+
            // lores: '5-6-7-8-9-10-11', // Length of residence: 0 (<1 year), 1 (1 year), 2 (2 years), ..., 15 (>14 years)
            marital: 'M', // Marital status: M (Married), S (Single), A (Inferred Married), B (Inferred Single)
            propertype: 10
        },
    },
    "solar_installer" : {
        api_url : "http://list.melissadata.net/v2/Property",
        api_key : 120506494,
        criteria: {
            occu: '1', // Ownership types: O (Owner Occupied), R (Renter Occupied), V (Vacant)
            hhage: '3-4', // Age ranges: 1 (18-24), 2 (25-34), 3 (35-44), 4 (45-54), 5 (55-64), 6 (65-74), 7 (75+)
            gender: 'M', // Gender: M (Male), F (Female), U (Unisex)
            // hhinc: 'C-D-E-F', // Income ranges: 1 (<$10,000), 2 ($10,000-14,999), 3 ($15,000-19,999), 4 ($20,000-24,999),
            //     // 5 ($25,000-29,999), 6 ($30,000-34,999), 7 ($35,000-39,999), 8 ($40,000-44,999),
            //     // 9 ($45,000-49,999), A ($50,000-54,999), B ($55,000-59,999), C ($60,000-64,999),
            //     // D ($65,000-69,999), E ($70,000-99,999), F ($100,000-149,999), G ($150,000-174,999),
            //     // H ($175,000-199,999), I ($200,000-249,999), J ($250,000+)
            people: '2-3-4', // Persons in household: 1, 2, 3, 4, 5, 6, 7, 8, 9, 10+
            // lores: '5-11', // Length of residence: 0 (<1 year), 1 (1 year), 2 (2 years), ..., 15 (>14 years)
            marital: 'M', // Marital status: M (Married), S (Single), A (Inferred Married), B (Inferred Single)
            propertype: 10
        },
    },
    "default" : {
        api_url : "https://api.melissa.com/leadgen/property/v1/getcount",
        api_key : 120506494,
        criteria: {
            ownertype: 'O', // Ownership types: O (Owner Occupied), R (Renter Occupied), V (Vacant)
            hhage: '5-6', // Age ranges: 1 (18-24), 2 (25-34), 3 (35-44), 4 (45-54), 5 (55-64), 6 (65-74), 7 (75+)
            hhinc: 'C-D-E-F', // Income ranges: 1 (<$10,000), 2 ($10,000-14,999), 3 ($15,000-19,999), 4 ($20,000-24,999),
                // 5 ($25,000-29,999), 6 ($30,000-34,999), 7 ($35,000-39,999), 8 ($40,000-44,999),
                // 9 ($45,000-49,999), A ($50,000-54,999), B ($55,000-59,999), C ($60,000-64,999),
                // D ($65,000-69,999), E ($70,000-99,999), F ($100,000-149,999), G ($150,000-174,999),
                // H ($175,000-199,999), I ($200,000-249,999), J ($250,000+)
            people: '3', // Persons in household: 1, 2, 3, 4, 5, 6, 7, 8, 9, 10+
            lores: '5-11', // Length of residence: 0 (<1 year), 1 (1 year), 2 (2 years), ..., 15 (>14 years)
            marital: 'M', // Marital status: M (Married), S (Single), A (Inferred Married), B (Inferred Single)
        },
    },
}
