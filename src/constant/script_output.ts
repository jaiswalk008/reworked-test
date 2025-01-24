export interface ScriptOutput {
    success: string,
    OriginalFilePath: string,
    NewFilePath: string,
    error: string,
    error_details: string, 
    mapped_cols: object,
    zipcode_sorted_list: object,
    criteria: object
}
