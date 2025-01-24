// __tests__/my-test.test.ts
// Import the function or component you want to test
import { calculateRowsLeftForUser, generateApiKey, getOriginalFileName, getPriceFromRange } from '../helper';
// Import necessary dependencies and the function to be tested
import { FileHistoryRepository } from '../repositories'; // Import the actual path

// import { queue } from '../helper/queue'; // Adjust the path accordingly
test('getOriginalFileName should return the correct result', async () => {

  // Call the function with the inputs
  const result = await getOriginalFileName('https://drive.google.com/uc?export=download&id=1MRyDLDBibbQ7qrKs0mFWAGbFQ-wWEnU9');
  
  // Assert the expected output
  expect(result).toBe('small_file_datatree.csv');
});
// test('generateApiKey should return the correct result', () => {
//   // Define input values

//   // Call the function with the inputs
//   const result = generateApiKey();

//   // Assert the expected output
//   expect(result).toBe('formattedRandomPart');
// });
test('getPriceFromRange should return the correct result', () => {
  // Define input values
  const price = [
    {
      costPerRow: 0.10,
      range: [100, 9100],
    },
    {
      costPerRow: 0.09,
      range: [9100, 24100],
    },
    {
      costPerRow: 0.08,
      range: [24100],
    }
  ]
  // Call the function with the inputs
  const result = getPriceFromRange(100, price);
  console.log('resultresult', result)
  // Assert the expected output
  expect(result).toBe(10);
});


// Mock the dependencies or create mock functions for them
const mockFindByEmail = {
  pricing_plan: {
    plan: 'PAYASYOUGO',
    stripe_subscription_status: 'active',
  },
  row_credits: 50, // Set the desired row credits
};


// Mock the FileHistoryRepository methods
const mockFileHistoryRepository: Partial<FileHistoryRepository> = {};

// Use the mock in your test
test('calculateRowsLeftForUser should return the correct values', async () => {
  // Assuming you have a function to test that uses the repository
  const result = await calculateRowsLeftForUser(mockFindByEmail, mockFileHistoryRepository as any);
  console.log("resultresult", result)
  // Add your assertions based on the expected behavior
  expect(result.totalAllowedRowCount).toBe(50);
  expect(result.remainingRowsForMonth).toBe(0);

  // Verify if the mocked method was called
  // expect(mockFileHistoryRepository.creditsUsedforCurrentBillingCycle).toHaveBeenCalled();
});
