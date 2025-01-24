const jwt = require('jsonwebtoken');

// Main function for processing Stripe payment
export const extractPythonResponse = async (args: any) => {
    const { python_output } = args;
    const regex = /{[^}]*'success'\s*:\s*('True'|'False')/i;
    const jsonRegex = /({.*})/; // Second regex to isolate JSON content

    let output;
    let timestamp: string | null = null;

    if (Array.isArray(python_output)) {
        const matchedElement = python_output.find((element: any) => {
            if (typeof element === 'string') {
                const match = JSON.stringify(element).match(regex);
                if (match) {
                    // Extract timestamp from the log entry (Assumes timestamp is at the beginning)
                    const timestampMatch = element.match(/^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2},\d{3})/);
                    if (timestampMatch) {
                        timestamp = timestampMatch[1];
                    }
                }
                return !!match;
            }
            return false;
        });

        if (matchedElement) {
            // Apply the second regex to extract JSON only
            const jsonMatch = matchedElement.match(jsonRegex);
            if (jsonMatch) {
                const sanitizedElement = jsonMatch[1].replace(/'/g, '"');
                try {
                     output = JSON.parse(sanitizedElement);

                    // Include timestamp if available
                    if (timestamp) {
                        output.timestamp = timestamp;
                    }
                } catch (error) {
                    console.error("Error parsing JSON:", error);
                    output = {
                        error_details: "Error parsing JSON",
                        success: false,
                        timestamp: timestamp || 'Unknown',
                    };
                }
            } else {
                output = {
                    error_details: `JSON content not found in the matched element- ${matchedElement}`,
                    success: false,
                    timestamp: timestamp || 'Unknown',
                };
            }
        } else {
            output = {
                error_details: `Output from script is not properly formatted- ${python_output}`,
                success: false,
                timestamp: timestamp || 'Unknown',
            };
        }
    } else {
        output = {
            error_details: `Output from script is not an array- ${python_output}`,
            success: false,
            timestamp: timestamp || 'Unknown',
        };
    }

    return { output };
};



export const extractDetailsFromAuthToken = (authHeader: string) => {
  if (!authHeader?.startsWith('Bearer ')) {
    throw new Error('Invalid or missing authorization header');
  }

  const token = authHeader.slice(7); // Remove 'Bearer ' prefix
  const decodedToken = jwt.decode(token) as { email?: string } | null;

  if (!decodedToken || !decodedToken.email) {
    throw new Error('Invalid token or email not found');
  }

  return { email: decodedToken.email };
};
