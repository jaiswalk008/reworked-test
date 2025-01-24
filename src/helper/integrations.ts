const { TwitterApi } = require('twitter-api-v2');

const adjectives = [
  "Amazing",
  "Incredible",
  "Mighty",
  "Fantastic",
  "Spectacular",
  "Unstoppable",
  "Invincible",
  "Legendary",
  "Epic",
  "Powerful"
];

const nouns = [
  "Avenger",
  "Hero",
  "Champion",
  "Defender",
  "Guardian",
  "Warrior",
  "Crusader",
  "Sentinel",
  "Vigilante",
  "Savior"
];
const wittyPhrases = [
  "Another day, another hero! 💥",
  "Checkout, our financial crusader! 💰",
  "Our guardian of savings has struck again! 💸",
  "Saving the day, one dollar at a time! 💪",
  "In the battle against expenses, our champion reigns supreme! 🛡️",
  "Savings, assemble! 💼",
  "With great financial responsibility comes great savings! 💡",
  "The hero we need, saving where it counts! 💼",
  "Protecting bottomline, one heroic act at a time! 💰",
  "Not all heroes wear capes, some just manage their finances wisely! 🦸‍♂️💸"
];
const generateMarvelName = () => {
  const randomAdjective = adjectives[Math.floor(Math.random() * adjectives.length)];
  const randomNoun = nouns[Math.floor(Math.random() * nouns.length)];
  return `${randomAdjective} ${randomNoun}`;
}


const twitterClient = new TwitterApi({
  appKey: process.env.API_KEY,
  appSecret: process.env.API_SECRET,
  accessToken: process.env.ACCESS_TOKEN_KEY,
  accessSecret: process.env.ACCESS_TOKEN_SECRET,
});

export const sendTwit: any = async (savings: string) => {
  try {
    const name = generateMarvelName();
    const randomPhrase = wittyPhrases[Math.floor(Math.random() * wittyPhrases.length)];
    const formattedSavings = new Intl.NumberFormat('en-US', { style: 'currency', currency: 'USD' }).format(parseInt(savings));
    const twit = `🌟 Congratulations to ${name}! Another ReWorked.ai customer saved: ${formattedSavings}, ${randomPhrase}`;
    console.log("Inside sendTwit, sending twit", twit)
    await twitterClient.v2.tweet(twit);
  } catch (error) {
    console.log("Error in sending twit", error.message)
  }
}
