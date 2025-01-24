export const industryTypes: any = {
    REAL_ESTATE_INVESTORS: "real_estate_investors",
    SOLAR_INSTALLER: "solar_installer",
    INSURANCE_PROVIDER : "insurance_provider",
    ROOFING : "roofing",
    AVAILABLE_INDUSTRIES:["real_estate_investors"]
}

export const industrTypesMetaData: any = {
    "real_estate_investors" : {
        ml_file : "pseudo_ml_final",
        threshold : 100,
        savings_calculation_factor: 0.67,
        skip_threshold_check : false 
    },
    "solar_installer" : {
        ml_file : "solar_ml",
        threshold : 30,
        savings_calculation_factor: 10,
        skip_threshold_check : true
    },
    "insurance_provider" : {
        ml_file : "insurance_ml",
        threshold : 24,
        savings_calculation_factor: 0.67,
        skip_threshold_check : true 
    },
    "roofing" : {
        ml_file : "roofing_ml",
        threshold : 30,
        savings_calculation_factor: 0.67,
        skip_threshold_check : false 
    },
}
