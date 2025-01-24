export type ConfidenceInsights = {
  confidence_score: number | null;
  actual_ml_calling_count: number | null;
  demo_address_verification_failed_percentage: number | null;
  demo_currently_lives_in_address_percentage: number | null;
  final_confidence_score: number | null;
  full_name_coverage: number | null, 
  zipcode_coverage: number | null, 
  age_source_melissa: number | null, 
  percentage_total_below_threshold: number | null
};
