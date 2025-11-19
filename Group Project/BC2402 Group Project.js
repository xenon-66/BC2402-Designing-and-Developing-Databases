//BC2402 Group Project NoSQL Script File
//Seminar 4 Group 1

use BC2402_GP

//1. [daily_intake] What are the countries with the greatest increase in carbohydrate intake over time?
db.daily_intake.aggregate([
    {
        $sort: {Year: 1} //sort the entire collection by year
    },
    {
        $group: {
            _id: "$Entity", 
            initial_carbs: { $first: { $toDouble: "$Daily_calorie_carbohydrates" } },
            final_carbs: { $last: { $toDouble: "$Daily_calorie_carbohydrates" } }
        } //group records by entity then select carb intake of earliest and latest years
    },
    {
        $project: {
            _id: 0,
            Entity: "$_id",
            carb_increase: {$round: [{$subtract: ["$final_carbs", "$initial_carbs"]}, 2]}
        } //compute increment in carb intake
    },
    {
        $sort: {carb_increase: -1} //sort results by largest increment 
    }
    ]);
    
/*logic: sort the entire dataset in ascending order of year so that, within each country, older records appear before newer one; group the data by country and extract the carbphydrate 
value from the first record (representing the earliest year) and the last record (representing the latest year); calculate the increment in carbphydrate intake by subtracting the earliest
calue form the latest value and rounding the result to 2dp; sort all countries in descending order based on this increment to see which countries experienced the largest increase in carbohydrate
intake over time*/


//2. [daily_intake] What are the average macronutrient intake for the last 10 years by country?
db.daily_intake.aggregate([
    {
        //convert required fields for proper calulcations
        $addFields: {
            year_int: {$toInt: "$Year"},
            animal_protein_calorie: {$toDouble: "$Daily_calorie_animal_protein" },
            vegetal_protein_calorie: {$toDouble: "$Daily_calorie_vegetal_protein" },
            fat_calorie: {$toDouble: "$Daily_calorie_fat" },
            carbs_calorie: {$toDouble: "$Daily_calorie_carbohydrates" }
        }
    },
    {
        //sort by entity then year ascendingly
        $sort: {Entity: 1, year_int: 1}
    },
    {
        //group all records per entity
        $group: {
            _id: "$Entity",
            last10: {
                $push: {
                    animal_protein_calorie: "$animal_protein_calorie",
                    vegetal_protein_calorie: "$vegetal_protein_calorie",
                    fat_calorie: "$fat_calorie",
                    carbs_calorie: "$carbs_calorie"
                }
            }
        }
    },
    {
        //keep only the most recent 10 years of macronutrients
        $project: {
            Entity: "$_id",
            last10: {$slice: ["$last10", -10]}
        }
    },
    {
        //compute averages
        $project: {
            _id: 0,
            Entity: 1,
            avg_animal_protein_calorie: {$round: [{$avg: "$last10.animal_protein_calorie"}, 2]},
            avg_vegetal_protein_calorie: {$round: [{$avg: "$last10.vegetal_protein_calorie"}, 2]},
            avg_fat_calorie: {$round: [{$avg: "$last10.fat_calorie"}, 2]},
            avg_carbs_calorie: {$round: [{$avg: "$last10.carbs_calorie"}, 2]},
        }
    },
    {
        //sort according to entity ascendingly
        $sort: {Entity: 1}
    }
    ]);

/*logic: converts all year and macronutrient fields to numeric types to allow proper sorting and calculations; sorts the dataset by country and year so that each country's records
are ordered chronologically before grouping all records by country and collecting the macronutrient values into an array; by slicing the last 10 elements of the array, it isolates 
the nutrient values correponding to the country's most recent 10 years; compute the average values for animal protein calorie, vegetal protein calorie, fat calorie and carbohydrate
calories acrros these 10 years and output the results sorted alphabetically by country*/


// 5.[simulated_food_intake_2015_2020] List the average monthly intake by nutrient.
// I convert 'Month' to an integer using $toInt so grouping and sorting are numeric, not lexical.
db.simulated_food_intake_2015_2020.aggregate([
  // 1) Normalise types so we can aggregate numerically
  {
    $project: {
    // Month is stored as a string -> convert to int so 1..12 sorts correctly
      Month: {$toInt: "$Month"},

    // Each nutrient is a string -> convert to double (strip commas just in case)
      animal_num: {
        $toDouble: {
          $replaceAll: {
            input: {$trim: {input: {$toString: "$Daily_calorie_animal_protein" } } },
            find: ",", replacement: ""
          }
        }
      },
      vegetal_num: {
        $toDouble: {
          $replaceAll: {
            input: {$trim: {input: {$toString: "$Daily_calorie_vegetal_protein" } } },
            find: ",", replacement: ""
          }
        }
      },
      fat_num: {
        $toDouble: {
          $replaceAll: {
            input: {$trim: {input: {$toString: "$Daily_calorie_fat" } } },
            find: ",", replacement: ""
          }
        }
      },
      carb_num: {
        $toDouble: {
          $replaceAll: {
            input: { $trim: { input: { $toString: "$Daily_calorie_carbohydrates" } } },
            find: ",", replacement: ""
          }
        }
      }
    }
  },

  // 2) Group by Month and compute the averages (now truly numeric)
  {
    $group: {
      _id: "$Month",
      avg_animal_protein:  { $avg: "$animal_num" },
      avg_vegetal_protein: { $avg: "$vegetal_num" },
      avg_fat:             { $avg: "$fat_num" },
      avg_carbohydrates:   { $avg: "$carb_num" }
    }
  },

  // 3) Match the exact output columns & names required by Q5
  {
    $project: {
      _id: 0,
      Month: "$_id",
      avg_animal_protein: 1,
      avg_vegetal_protein: 1,
      avg_fat: 1,
      avg_carbohydrates: 1
    }
  },

  // 4) Required sort: Month ascending
  { $sort: { Month: 1 } }
]);

//6.[simulated_food_intake_2015_2020] Consider 'United States', 'India', 'Germany', 'Brazil', 'Japan'. Identify the corresponding seasonal spikes (month) in intake.
/*
Approach:
   - I normalise Month and nutrients to numbers.
   - For each nutrient, I sort by nutrient DESC within Entity, then $group to take the first Month.
   - I do this in parallel with $facet, then merge the four results by Entity.
*/

db.simulated_food_intake_2015_2020.aggregate([
  // 1) Keep only the 5 countries 
  {
    $match: {
      Entity: {$in: ['United States','India','Germany','Brazil','Japan']}
    }
  },

  // 2) Normalise types so I can sort numerically (all raw fields are strings)
  {
    $project: {
      Entity: 1,
      Month: {$toInt: "$Month"}, // ensure numeric month
      fat: {
        $toDouble: {
          $replaceAll: {input: {$toString: "$Daily_calorie_fat"}, find: ",", replacement: ""}
        }
      },
      animal: {
        $toDouble: {
          $replaceAll: {input: {$toString: "$Daily_calorie_animal_protein"}, find: ",", replacement: ""}
        }
      },
      vegetal: {
        $toDouble: {
          $replaceAll: {input: {$toString: "$Daily_calorie_vegetal_protein"}, find: ",", replacement: ""}
        }
      },
      carbs: {
        $toDouble: {
          $replaceAll: {input: {$toString: "$Daily_calorie_carbohydrates"}, find: ",", replacement: ""}
        }
      }
    }
  },

  // 3) In parallel ($facet), I compute month-of-maximum for each nutrient.
  // sort by Entity ASC, nutrient DESC then group by Entity and take the first Month.
  {
    $facet: {
      fat: [
        {$sort: {Entity: 1, fat: -1, Month: 1}},
        {$group: {_id: "$Entity", peak_month_fat: {$first: "$Month"}}}
      ],
      animal: [
        {$sort: {Entity: 1, animal: -1, Month: 1 } },
        {$group: {_id: "$Entity", peak_month_animal_protein: {$first: "$Month"}}}
      ],
      vegetal: [
        {$sort: {Entity: 1, vegetal: -1, Month: 1}},
        {$group: {_id: "$Entity", peak_month_vegetal_protein: {$first: "$Month"}}}
      ],
      carb: [
        {$sort: {Entity: 1, carbs: -1, Month: 1}},
        {$group: {_id: "$Entity", peak_month_carbohydrates: {$first: "$Month"}}}
      ]
    }
  },

  // 4) I merge the four arrays into one document per Entity.
  // I unwind 'fat' as the driver, then pick matching documents from the other arrays by _id.
  {
    $unwind: "$fat"
  },
  {
    $project: {
      Entity: "$fat._id",
      peak_month_fat: "$fat.peak_month_fat",

      // I pick the matching entity from each sibling array using $filter + $first
      peak_month_animal_protein: {
        $let: {
          vars: {
            m: {
              $first: {
                $filter: {input: "$animal", as: "a", cond: {$eq: ["$$a._id", "$fat._id"]}}
              }
            }
          },
          in: "$$m.peak_month_animal_protein"
        }
      },
      peak_month_vegetal_protein: {
        $let: {
          vars: {
            m: {
              $first: {
                $filter: {input: "$vegetal", as: "v", cond: {$eq: ["$$v._id", "$fat._id"]}}
              }
            }
          },
          in: "$$m.peak_month_vegetal_protein"
        }
      },
      peak_month_carbohydrates: {
        $let: {
          vars: {
            m: {
              $first: {
                $filter: {input: "$carb", as: "c", cond: {$eq: ["$$c._id", "$fat._id"]}}
              }
            }
          },
          in: "$$m.peak_month_carbohydrates"
        }
      }
    }
  },

  // 5) Final sort to match rubric
  {$sort: {Entity: 1}}
]);


//#9
db.mcdonaldata.aggregate([
  {
    $project: {
      item: 1,
      totalfat: { $toDouble: "$totalfat" },
      protien: { $toDouble: "$protien" },
      fat_to_protien_ratio: {
        $cond: {
          if: { $eq: [{ $toDouble: "$protien" }, 0] },
          then: null,  // skip or mark as null when protein = 0
          else: { $divide: [{ $toDouble: "$totalfat" }, { $toDouble: "$protien" }] }
        }
      }
    }
  },
  {
    $sort: { fat_to_protein_ratio: -1 }
  }
])


//#10
db.mcdonaldata.aggregate([
  {
    $project: {
      item: 1,
      calories: 1,
      totalfat: 1,
      cholestrol: 1,
      sodium: 1,
      health_flag: {
        $cond: {
          if: {
            $or: [
              { $gt: [{ $toDouble: "$totalfat" }, 30] },
              { $gt: [{ $toDouble: "$sodium" }, 1000] },
              { $gt: [{ $toDouble: "$cholestrol" }, 30] }
            ]
          },
          then: "High Risk",
          else: "Moderate"
        }
      }
    }
  },
  { $sort: { health_flag: 1 } }
]);


// Q11. [burger_king_menu] Which Categories Are Most Weight Watchers-Friendly?

db.burger_king_menu.find()

db.burger_king_menu.find().count()
// Count total documents - 77 rows

db.burger_king_menu.distinct("Item").length
// Count distinct Items - 73 rows
// There are 4 duplicates in the burger_king_menu dataset

db.burger_king_menu.aggregate([
  { // Groups all documents by the Item field and counts how many times each value appears
    $group: {
      _id: "$Item",
      count: {$sum: 1} 
    }
  },
  { // Keeps only those items with a count greater than 1
    $match: {
      count: {$gt: 1} 
    }
  },
  { // Sorts results alphabetically by the item name
    $sort: {_id: 1} 
  }
])
// Find duplicated items

db.burger_king_menu.find({
  Item: { // Retrieves all documents from the burger_king_menu collection where the field Item matches those in the list
    $in: ["Hamburger", "Cheeseburger", "Chicken Nuggets- 4pc", "Chicken Nuggets- 6pc"] 
  }
}).sort({Item: 1})
// To see the actual duplicate documents
// Identified the duplicated rows. Cheeseburger & Hamburger are completely duplicated. 
// Chicken Nuggets (4pc & 6pc) have a copy of each but 1 category is Chicken while the 
// other is Burgers, other nutritional value is the same.
// Action needed: Drop the duplicates for the 2 burgers & drop the duplicate for nuggets 
// if the category is Burgers.

db.burger_king_menu.find({
  $nor: [
    {Item: {$regex: /Chicken Nuggets/}, Category: "Burgers"}
  ]
})
// This query does not include the rows where Chicken Nuggets have the burgers category.
// However, Hamburger and Cheeseburger duplicates still exist. 
// The deduplication happens in the aggregation stage using $group by Item later on.

db.burger_king_menu.aggregate([
  { // Exclude Chicken Nuggets under Burgers
    $match: {
      $nor: [
        {Item: {$regex: /Chicken Nuggets/}, Category: "Burgers"}
      ]
    }
  },
  { // Here, we group by the columns (take out duplicates)
    $group: {
      _id: {
        Item: "$Item",
        Category: "$Category",
        Weight_Watchers: "$Weight_Watchers"
      }
    }
  },
  { // Flatten fields back into a clean structure and convert Weight_Watchers to numeric for averaging.
    $project: {
      _id: 0,
      Category: "$_id.Category",
      Weight_Watchers: {
        $convert: {
          input: "$_id.Weight_Watchers",
          to: "double"
        }
      }
    }
  },
  { // Group by Category and compute average Weight_Watchers
    $group: {
      _id: "$Category",
      Avg_WW_Score: {$avg: "$Weight_Watchers"}
    }
  },
  { // Sort by average Weight Watchers score
    $sort: {Avg_WW_Score: 1}
  },
  { // Project final output (Category, Avg_WW_Score)
    $project: {
      _id: 0, // hide the _id field
      Category: "$_id", // display category
      Avg_WW_Score: "$Avg_WW_Score" // display avergae weight watcher score
    }
  }
])
// This is the final query ordered by the average weight watcher score. 
// Converted Weight_Watchers to double as its stored as text.


// Q12. [burger_king_menu] List the top 10 most caloric menu items.

db.burger_king_menu.aggregate([
  { // Exclude Chicken Nuggets under Burgers
    $match: {
      $nor: [
        {Item: {$regex: /Chicken Nuggets/}, Category: "Burgers"}
      ]
    }
  },
  { // Group by the unique combination of Item + Category + Calories
    $group: {
      _id: {
        Item: "$Item",
        Category: "$Category",
        Calories: "$Calories"
      }
    }
  },
  { // Flatten the grouped fields back to top-level fields.
    $project: {
      _id: 0, // hide the _id field
      Item: "$_id.Item", // bring back Item
      Category: "$_id.Category", // bring back Category
      Calories: {
        $convert: {
          input: "$_id.Calories",
          to: "double" // convert to numeric type
        }
      }
    }
  },
  {$sort: {Calories: -1}}, // Sort the resulting rows by Calories in descending order
  {$limit: 10} // Limit output to only the top 10 rows after sorting
])
// Converted Calories to double as its stored as text. Without conversion, the sort 
// would sort alphabetically. We want it to sort numerically in descending order. 
// No conversion sort - 980, 900, 90, 1000. This is wrong if we are trying to find 
// the top 10 caloric items.


// Q16. Do countries with higher average protein/fat intake during winter months 
// tend to report higher happiness?

// Define winter months by hemisphere
// Northern Hemisphere (Dec, Jan, Feb): US, India, Germany, Japan
// Southern Hemisphere (Jun, Jul, Aug): Brazil

db.simulated_food_intake_2015_2020.aggregate([
    // add hemisphere classification
    {
        $addFields: {
            hemisphere: {
                $cond: {
                    if: {$eq: ["$Entity", "Brazil"]},
                    then: "Southern",
                    else: "Northern"
                }
            }
        }
    },
    // filter for winter months only
    {
        $match: {
            $or: [
                {hemisphere: "Northern", Month: {$in: ["12", "1", "2"]}},
                {hemisphere: "Southern", Month: {$in: ["6", "7", "8"]}}
            ]
        }
    },
    // calculate average winter intakes by country
    {
        $group: {
            _id: "$Entity",
            avg_winter_animal_protein: {
                $avg: {$toDouble: "$Daily_calorie_animal_protein"}
            },
            avg_winter_vegetal_protein: {
                $avg: {$toDouble: "$Daily_calorie_vegetal_protein"}
            },
            avg_winter_fat: {
                $avg: {$toDouble: "$Daily_calorie_fat"}
            }
        }
    },
    // lookup happiness data
    {
        $lookup: {
            from: "happiness",
            localField: "_id",
            foreignField: "Country",
            as: "happiness_data"
        }
    },
    // unwind to flatten happiness data
    {
        $unwind: "$happiness_data"
    },
    // project final output with calculated total protein
    {
        $project: {
            _id: 0,
            Country: "$_id",
            Happiness_Rank: "$happiness_data.Happiness_Rank",
            Happiness_Score: "$happiness_data.Happiness_Score",
            avg_winter_fat: 1,
            avg_winter_animal_protein: 1,
            avg_winter_vegetal_protein: 1,
            avg_winter_total_protein: {
                $add: ["$avg_winter_animal_protein", "$avg_winter_vegetal_protein"]
            }
        }
    },
    // sort by happiness score descending
    {
        $sort: {Happiness_Score: -1}
    }
])


//17
use bc2402_gp
db.daily_intake.aggregate([
  {
    $group: {
      _id: "$Entity",
      Average_Daily_calorie_fat: { $avg: { $toDouble: "$Daily_calorie_fat" } }
    }
  },
  {
    $lookup: {
      from: "happiness",
      localField: "_id",
      foreignField: "Country",
      as: "happiness_data"
    }
    
  },
  {
    $match: {
      happiness_data: { $elemMatch: { Country: { $exists: true } } }
    }
  },
  {
    $project: {
      _id: 0,
      Entity: "$_id",
      Average_Daily_calorie_fat: 1,
      Happiness_Rank: "$happiness_data.Happiness_Rank",
      Happiness_Score: "$happiness_data.Happiness_Score"
    }
  }
])


// 18. *Open-ended question [daily_intake] and [happiness] Are countries with lower nutrient variation happier?
/*
 Approach:
 I compute per-country standard deviation of *yearly* nutrient values, then
 average them and join to happiness.
*/

db.daily_intake.aggregate([
  {
    // Step 1: I select raw fields and capture *either* lower- or Title-Case keys.
    // Reason: dumps may use `daily_calorie_fat` or `Daily_calorie_fat`.
    // I park them in *_raw so I can normalize them in one place.
    $project: {
      Country: "$Entity",
      Year: {$toInt: "$Year"}, // Year must be numeric for any downstream numeric ops.
      _animal_raw: {$ifNull: ["$daily_calorie_animal_protein", "$Daily_calorie_animal_protein"]},
      _vegetal_raw: {$ifNull: ["$daily_calorie_vegetal_protein", "$Daily_calorie_vegetal_protein"]},
      _fat_raw: {$ifNull: ["$daily_calorie_fat", "$Daily_calorie_fat"]},
      _carb_raw: {$ifNull: ["$daily_calorie_carbohydrates", "$Daily_calorie_carbohydrates"]}
    }
  },
  {
    // Step 2: Convert each *_raw to a *numeric*:
    // (a) Force to string so I can clean it deterministically
    // (b) Treat "" and null as null (not a number)
    // (c) Remove thousand separators "," if present
    // (d) Cast the cleaned string to Double
    $addFields: {
      animal: {
        $let: {
          vars: {s: {$toString: "$_animal_raw"}}, // (a)
          in: {
            $cond: [
              {$or: [{$eq: ["$$s", ""]}, {$eq: ["$$s", null]}]}, // (b)
              null,
              {$toDouble: {$replaceAll: {input: "$$s", find: ",", replacement: ""}}} // (c)+(d)
            ]
          }
        }
      },
      vegetal: {
        $let: {
          vars: {s: {$toString: "$_vegetal_raw"}},
          in: {
            $cond: [
              {$or: [{$eq: ["$$s", ""]}, {$eq: ["$$s", null]}]},
              null,
              {$toDouble: {$replaceAll: {input: "$$s", find: ",", replacement: ""}}}
            ]
          }
        }
      },
      fat: {
        $let: {
          vars: {s: {$toString: "$_fat_raw"}},
          in: {
            $cond: [
              {$or: [{$eq: ["$$s", ""]}, {$eq: ["$$s", null]}]},
              null,
              {$toDouble: {$replaceAll: {input: "$$s", find: ",", replacement: ""}}}
            ]
          }
        }
      },
      carb: {
        $let: {
          vars: {s: {$toString: "$_carb_raw"}},
          in: {
            $cond: [
              {$or: [{$eq: ["$$s", ""]}, {$eq: ["$$s", null]}]},
              null,
              {$toDouble: {$replaceAll: {input: "$$s", find: ",", replacement: ""}}}
            ]
          }
        }
      }
    }
  },
  {
    // Step 3: Have to ensure at least one numeric field exists; otherwise std dev has no inputs.
    $match: {
      $or: [
        {animal: {$ne: null}},
        {vegetal: {$ne: null}},
        {fat: {$ne: null}},
        {carb: {$ne: null}}
      ]
    }
  },
  {
    // Step 4: Now that all four series are numeric (or null), std dev can aggregate.
    $group: {
      _id: "$Country",
      std_animal: {$stdDevSamp: "$animal"},
      std_vegetal: {$stdDevSamp: "$vegetal"},
      std_fat: {$stdDevSamp: "$fat"},
      std_carb: {$stdDevSamp: "$carb"}
    }
  },
  {
    // Step 5: I build a composite numeric index using only non-null numeric parts.
    // Rationale: if one series is entirely null for a country, exclude it from the mean.
    $addFields: {
      _parts: {
        $filter: {
          input: ["$std_animal", "$std_vegetal", "$std_fat", "$std_carb"],
          as: "x",
          cond: {$ne: ["$$x", null]} // keep only numeric results
        }
      }
    }
  },
  {
    // Step 6: Final numeric index = average of available std devs (pure numeric).
    $addFields: {
      avg_yearly_variation: {
        $cond: [
          {$gt: [{$size: "$_parts"}, 0]},
          {$divide: [{$sum: "$_parts"}, {$size: "$_parts"}]},
          null
        ]
      }
    }
  },
  {
    // Step 7: Join to happiness and coerce score to a numeric for sorting/plots.
    $lookup: {
      from: "happiness",
      localField: "_id",
      foreignField: "Country",
      as: "h"
    }
  },
  {$unwind: "$h"},
  {
    $project: {
      _id: 0,
      Country: "$_id",
      std_animal: 1,
      std_vegetal: 1,
      std_fat: 1,
      std_carb: 1,
      avg_yearly_variation: 1,
      Happiness_Score: {$toDouble: {$toString: "$h.Happiness_Score"}} // numeric score
    }
  },
  {
    // Step 8: Numeric sort: low variation first; tie-break by higher numeric happiness.
    $sort: {avg_yearly_variation: 1, Happiness_Score: -1}
  }
]);


/*19. Country Trends: Processed Food Intake vs. Fast Food Menu Health
Are there any relationships between processed food intake and fast-food menu? 
What can be the impact of processed food and fast-food consumption on happiness? 
What about health outcomes?*/

//Are there any relationships between processed food intake and fast-food menu?

//average macronutrients in burger king menu
db.burger_king_menu.aggregate([
  {
    $group: {
      _id: null,
      avg_calories: {$avg: {$toDouble: "$Calories"}},
      avg_fat: {$avg: {$toDouble: "$Fat_g"}},
      avg_carb: {$avg: {$toDouble: "$Total_Carb_g"}},
      avg_protein: {$avg: {$toDouble: "$Protein_g"}}
    }
  }
]);

//average macronutrients in starbucks menu 
db.starbucks.aggregate([
  {$match: {type: {$in: ["bakery", "bistro box", "hot breakfast", "sandwich"]}}}, //include only items that can be classified as a meal
  {
    $group: {
      _id: null,
      avg_calories: {$avg: {$toDouble: "$calories"}},
      avg_fat: {$avg: {$toDouble: "$fat"}},
      avg_carb: {$avg: {$toDouble: "$carb"}},
      avg_protein: {$avg: {$toDouble: "$protein"}}
    }
  }
]);

/*on average, the average macronutrients for a fast food meal is calorie dense, with carbohydrates comprising the largest proportion  of the macronutrients, followed by fat; protein 
content is the lowest among the 3 macronutrients identified

next step: to explore potential relationships between processed food intake and fast food menu, I will identify countries with high processed food intake using external data and 
analyse whetehr the macronutrien proportions align with those observed in an average fast-food meals*/

//source: https://www.bmj.com/content/383/bmj-2023-075294
//using external data, united states and united kingdom have the highest process food intake with 58% and 57% of the adult diet respectively
//finding the average macronutrient intake for united states and united kingdom
db.daily_intake.aggregate([
  {
    $match: {
        Entity: {$in: ["United States", "United Kingdom"]}
    }
  },
  {
    $group: {
        _id: "$Entity",
        avg_calorie_carb: {$avg: {$toDouble: "$Daily_calorie_carbohydrates"}},
        avg_calorie_fat: {$avg: {$toDouble: "$Daily_calorie_fat"}},
        avg_calorie_animal_protein: {$avg: {$toDouble: "$Daily_calorie_animal_protein"}}
    }
  },
  {
    $project: {
        _id: 0,
        entity: "$_id",
        avg_calorie_carb: {$round: ["$avg_calorie_carb", 2]},
        avg_calorie_fat: {$round: ["$avg_calorie_fat", 2]},
        avg_calorie_animal_protein: {$round: ["$avg_calorie_animal_protein", 2]}
    }
  }
]);

/*from the query above, the average calorie intake from carbohydrates is the highest, followed by calories from fat, with calories from animal 
protein being the lowest; this distribution aligns with the findings from the average marconutrient proportions in fast food meals across the 
fast food chains

hence, there appears to be a relationship between processed food intake and fast food menu, specifically there seems to be a positive relationship
between processed food intake and the macronutrient composition of fast food menu; the findings above suggest the countries with higher processed food
intake tend to have diets that mirror the macronutrient composition seen in fast food meals, which are higher carbohydrates and fat intake coupled with 
lower protein intake

overall, the findings suggest a positive relationship where higher processed food consumption is associated with a similar pattern of macronutient 
distribution in fast food meals*/

//What can be the impact of processed food and fast-food consumption on happiness? 
//source: https://www.bmj.com/content/383/bmj-2023-075294
//countries with highest processed/fast food intake: united states, united kingdom, canada, sweden, australia
//countries with low processed/fast food intake: romania, colombia, hungary, italy, estonia

//query for countries with high intake
db.happiness.aggregate([
    {$match: {Country: {$in: ["United States", "United Kingdom", "Canada", "Sweden", "Australia"]}}},
    {
        $group: {
            _id: null,
            avg_rank: {$avg: {$toDouble: "$Happiness_Rank"}},
            avg_score: {$avg: {$toDouble: "$Happiness_Score"}}
        }
    }
]);

//query for countries with low intake
db.happiness.aggregate([
    {$match: {Country: {$in: ["Romania", "Colombia", "Hungary", "Italy", "Estonia"] }}},
    {
        $group: {
            _id: null,
            avg_rank: {$avg: {$toDouble: "$Happiness_Rank"}},
            avg_score: {$avg: {$toDouble: "$Happiness_Score"}}
        }
    }
]);

/*from the queries above, it is observed that countries with higher processed and fast food intake tend to hold higher ranks on the happiness scale and
have higher happiness scores compared to countries with lower processed/fast food consumption; after averaging the happiness ranks and scores for each group,
countires with high processed/fast food intake have an average happiness rank of 11.8 and a happiness score of 7.21, which is significantly higher than the 
69.2 and 5.56 score for countries with low processed/fast food intake

this suggests that processed food and fast food consumption may have a positive impact on happiness; while other factors certainly contribute to happiness,
the findings imply that countries with higher consumption of processed and fast food benefit from greater overall happiness*/

//What about health outcomes?
//source: https://www.bmj.com/content/383/bmj-2023-075294
//countries with highest processed/fast food intake: united states, united kingdom, canada, sweden, australia
//countries with low processed/fast food intake: romania, colombia, hungary, italy, estonia

//query for countries with high intake
db.happiness.aggregate([
    {$match: {Country: {$in: ["United States", "United Kingdom", "Canada", "Sweden", "Australia"]}}},
    {
        $group: {
            _id: null,
            avg_life: {$avg: {$toDouble: "$Health_Life_Expectancy"}},
            avg_gdp: {$avg: {$toDouble: "$Economy_GDP_per_Capita"}},
            avg_family: {$avg: {$toDouble: "$Family"}},
            avg_freedom: {$avg: {$toDouble: "$Freedom"}}
        }
    }
]);

//query for countries with low intake
db.happiness.aggregate([
    {$match: {Country: {$in: ["Romania", "Colombia", "Hungary", "Italy", "Estonia"] }}},
    {
        $group: {
            _id: null,
            avg_life: {$avg: {$toDouble: "$Health_Life_Expectancy"}},
            avg_gdp: {$avg: {$toDouble: "$Economy_GDP_per_Capita"}},
            avg_family: {$avg: {$toDouble: "$Family"}},
            avg_freedom: {$avg: {$toDouble: "$Freedom"}}
        }
    }
]);

/*findings: countries with higher processed/fast food intake have a higher average life expectancy (0.90) as compared to countries with lower intake (0.78); 
countries with higher intake have a higher average gdp per capita (1.33) as compared to countries with lower intake (1.09); countries with higher intake have a higher
average family score (1.29) as compared to countries with lower intake (1.15); countries with higher intake have a higher average freedom score (0.61) as compared to
countries with lower intake (0.38)

conclusion: although fast food and processed food in general are often associated with negative health reprecussions, countries with high intake actually have a higher 
average life expectancy as compared to those with lower intakes, possibly suggesting that perhaps the impact of fast food may not be as drastic as commonly believed

however, the difference in average life expectancy could also be justified by other factors; for instance, countries with higher processed food intake also tend to have a 
higher gdp per capita on average, which may translate into better access to healthcare facilities and lifestyle amenities that help offset the negative effects of fast food, 
ultimately contributing to an overall net benefit on health; additionally, factors such as family support and freedom may also play a role in enhancing overall well-being and
health outcomes, further epxlaining the higher average life expectancy observed in countries with higher intake*/


//20. Does fast-food consumption increase health risk? Could the risk be mitigated?
db.burger_king_menu.aggregate([

  // -------------------------------------------------------
  // 1. Run TWO pipelines in parallel using $facet:
  //    - "baseline"  : current menu nutrients
  //    - "mitigated" : hypothetical reformulated menu
  // -------------------------------------------------------
  {
    $facet: {

      // ---------- BASELINE SCENARIO ----------
      baseline: [
        // 1a) Coerce string fields to numeric so we can do math
        {
          $addFields: {
            calories_num: { $toDouble: "$Calories" },     // kcal per serving
            sodium_num:   { $toDouble: "$Sodium_mg" },    // mg Na per serving
            fat_num:      { $toDouble: "$Fat_g" }         // g fat per serving
          }
        },

        // 1b) Compute Na and fat per 100 kcal
        {
          $addFields: {
            mgNa_per_100kcal: {
              // if calories > 0 → (sodium / calories) * 100, else null
              $cond: [
                { $gt: [ "$calories_num", 0 ] },
                { $multiply: [
                    { $divide: [ "$sodium_num", "$calories_num" ] },
                    100
                  ]},
                null
              ]
            },
            gFat_per_100kcal: {
              $cond: [
                { $gt: [ "$calories_num", 0 ] },
                { $multiply: [
                    { $divide: [ "$fat_num", "$calories_num" ] },
                    100
                  ]},
                null
              ]
            }
          }
        },

        // 1c) Flag items that exceed Na / fat thresholds
        //     (you can adjust the cut-offs to match your SQL logic)
        {
          $addFields: {
            high_na:  { $gt: [ "$mgNa_per_100kcal", 400 ] },  // TRUE if Na is high
            high_fat: { $gt: [ "$gFat_per_100kcal", 4.5 ] }   // TRUE if fat is high
          }
        },

        // 1d) Convert the two boolean flags into a single risk_score:
        //     2 = high Na AND high fat
        //     1 = high Na OR high fat (but not both)
        //     0 = neither
        {
          $addFields: {
            risk_score: {
              $switch: {
                branches: [
                  {
                    case: { $and: [ "$high_na", "$high_fat" ] },
                    then: 2
                  },
                  {
                    case: { $or: [ "$high_na", "$high_fat" ] },
                    then: 1
                  }
                ],
                default: 0
              }
            }
          }
        },

        // 1e) Count how many items fall into each risk_score bucket
        {
          $group: {
            _id: "$risk_score",
            item_count: { $sum: 1 }
          }
        },

        // 1f) Reshape fields into a clean structure
        {
          $project: {
            _id: 0,
            scenario:   { $literal: "baseline" }, // label this pipeline
            risk_score: "$_id",
            item_count: 1
          }
        }
      ],

      // ---------- MITIGATED SCENARIO ----------
      mitigated: [
        // 2a) Coerce to numbers again (same as baseline)
        {
          $addFields: {
            calories_num: { $toDouble: "$Calories" },
            sodium_num:   { $toDouble: "$Sodium_mg" },
            fat_num:      { $toDouble: "$Fat_g" }
          }
        },

        // 2b) Apply a simple “reformulation” assumption:
        //     e.g. 20% less sodium and 15% less fat before
        //     computing per-100kcal metrics.
        {
          $addFields: {
            mgNa_per_100kcal_new: {
              $cond: [
                { $gt: [ "$calories_num", 0 ] },
                {
                  $multiply: [
                    { $divide: [ "$sodium_num", "$calories_num" ] },
                    100,
                    0.8                // 20% reduction in Na
                  ]
                },
                null
              ]
            },
            gFat_per_100kcal_new: {
              $cond: [
                { $gt: [ "$calories_num", 0 ] },
                {
                  $multiply: [
                    { $divide: [ "$fat_num", "$calories_num" ] },
                    100,
                    0.85               // 15% reduction in fat
                  ]
                },
                null
              ]
            }
          }
        },

        // 2c) Re-apply the same Na / fat high-risk cut-offs
        {
          $addFields: {
            high_na:  { $gt: [ "$mgNa_per_100kcal_new", 400 ] },
            high_fat: { $gt: [ "$gFat_per_100kcal_new", 4.5 ] }
          }
        },

        // 2d) Compute mitigated risk_score
        {
          $addFields: {
            risk_score: {
              $switch: {
                branches: [
                  {
                    case: { $and: [ "$high_na", "$high_fat" ] },
                    then: 2
                  },
                  {
                    case: { $or: [ "$high_na", "$high_fat" ] },
                    then: 1
                  }
                ],
                default: 0
              }
            }
          }
        },

        // 2e) Count items per risk_score
        {
          $group: {
            _id: "$risk_score",
            item_count: { $sum: 1 }
          }
        },

        // 2f) Same tidy structure, but scenario = mitigated
        {
          $project: {
            _id: 0,
            scenario:   { $literal: "mitigated" },
            risk_score: "$_id",
            item_count: 1
          }
        }
      ]
    }
  },

  // -------------------------------------------------------
  // 2. Combine the baseline and mitigated arrays
  //    produced by $facet into a single array "results"
  // -------------------------------------------------------
  {
    $project: {
      results: { $concatArrays: [ "$baseline", "$mitigated" ] }
    }
  },

  // -------------------------------------------------------
  // 3. Turn each element of "results" into its own document
  //    so we can group / project like normal
  // -------------------------------------------------------
  { $unwind: "$results" },

  // After $unwind, each doc looks like:
  //   { results: { scenario: "baseline", risk_score: 2, item_count: 18 } }
  // We now *promote* that inner object to be the root document.
  {
    $replaceRoot: {
      newRoot: "$results"
    }
  },

  // -------------------------------------------------------
  // 4. Compute total items per scenario so we can get %
  //    and then expand back to one row per (scenario, risk_score)
  // -------------------------------------------------------
  {
    $group: {
      _id: "$scenario",
      total_items: { $sum: "$item_count" },
      breakdown: {
        $push: {
          risk_score: "$risk_score",
          item_count: "$item_count"
        }
      }
    }
  },

  // One document per scenario, now expand the breakdown array
  { $unwind: "$breakdown" },

  // -------------------------------------------------------
  // 5. Final shape:
  //    scenario | risk_score | item_count | pct_items
  // -------------------------------------------------------
  {
    $project: {
      _id: 0,
      scenario:   "$_id",
      risk_score: "$breakdown.risk_score",
      item_count: "$breakdown.item_count",
      pct_items: {
        // percentage of items in this risk bucket, rounded to 2 dp
        $round: [
          {
            $multiply: [
              { $divide: [ "$breakdown.item_count", "$total_items" ] },
              100
            ]
          },
          2
        ]
      }
    }
  },

  // Sort nicely: baseline first, then mitigated;
  // inside each scenario, show High (2) above Medium (1) above Low (0)
  { $sort: { scenario: 1, risk_score: -1 } }

]);


//#21 Part 1
// So here we want to find the overall global trends first and to see which variables we should be focusing on
// Specifically we want to find the avg calorie consumption since the daily intake has yearly data but happines doesnt

db.daily_intake.aggregate([
  //Convert all relevant intake fields from string to double so we can calculate averages and total later
  {
    $project: {
      Entity: 1,
      fat: { $toDouble: "$Daily_calorie_fat" },
      animal_protein: { $toDouble: "$Daily_calorie_animal_protein" },
      vegetal_protein: { $toDouble: "$Daily_calorie_vegetal_protein" },
      carbs: { $toDouble: "$Daily_calorie_carbohydrates" }
    }
  },
  //Group by Entity (country) and calculate the average for each calorie intake type
  {
    $group: {
      _id: "$Entity",
      avg_fat: { $avg: "$fat" },
      avg_animal_protein: { $avg: "$animal_protein" },
      avg_vegetal_protein: { $avg: "$vegetal_protein" },
      avg_carbs: { $avg: "$carbs" }
    }
  },
  // Round the calculated averages to 2 decimal places so it looks cleaner
  {
    $project: {
      _id: 1,
      avg_fat_intake: { $round: ["$avg_fat", 2] },
      avg_animal_protein_intake: { $round: ["$avg_animal_protein", 2] },
      avg_vegetal_protein_intake: { $round: ["$avg_vegetal_protein", 2] },
      avg_carbohydrate_intake: { $round: ["$avg_carbs", 2] }
    }
  },
  // Join the daily_intake dataset with the happiness collection
  {
    $lookup: {
      from: "happiness",
      localField: "_id",
      foreignField: "Country",    // The country data in happiness is the equivalent of entity in daily_intake
      as: "happiness_data"    // The name for the new dataset that was built out of the happiness dataset
    }
  },
  {
    $unwind: "$happiness_data"
  },
  // Show the final document structure and calculate total intake
  {
    $project: {
      _id: 0,
      country: "$_id",
      Economy_GDP_per_Capita: { $toDouble: "$happiness_data.Economy_GDP_per_Capita" },
      Health_Life_Expectancy: { $toDouble: "$happiness_data.Health_Life_Expectancy" },
      Family: { $toDouble: "$happiness_data.Family" },
      avg_fat_intake: 1,
      avg_animal_protein_intake: 1,
      avg_vegetal_protein_intake: 1,
      avg_carbohydrate_intake: 1,
      avg_total_calorie_intake: {
        $round: [
          {
            $add: [
              "$avg_fat_intake",
              "$avg_animal_protein_intake",
              "$avg_vegetal_protein_intake",
              "$avg_carbohydrate_intake"
            ]
          },
          2
        ]
      }
    }
  },
  //sort by the average total calorie intake per day
  {
    $sort: {
      avg_total_calorie_intake: -1 //
    }
  }
])


//Q21 Part 2
// So here I was trying to display only the relevant data for the USA after sorting the global data from before.
// The question specified looking into US data specifically
db.daily_intake.aggregate([
  {
    $match: {
      Entity: "United States"
    }
  },
  
  {
    $project: {
      _id: 0, 
      Year: 1,
      Entity: 1,
      Daily_calorie_animal_protein: 1,
      Daily_calorie_vegetal_protein: 1,
      Daily_calorie_fat: 1,
      Daily_calorie_carbohydrates: 1,
      total_daily_calories: {
        $add: [
            { $toDouble: "$Daily_calorie_animal_protein" },
            { $toDouble: "$Daily_calorie_vegetal_protein" },
            { $toDouble: "$Daily_calorie_fat" },
            { $toDouble: "$Daily_calorie_carbohydrates" }
        ]
      }
    }
  },
  

  {
    $sort: {
      Year: -1
    }
  }
])


// Q22. *Blue-sky question What months should governments increase public awareness of unhealthy food spikes?
// For example, are there healthy fast-food options that can be promoted via public campaigns?
// What healthy fast-food options can be introduced? What makes these options suitable?

db.mcdonaldata.find()
db.simulated_food_intake_2015_2020.find()

// Identify unhealthy food spike months
db.simulated_food_intake_2015_2020.aggregate([
    { // Convert text fields to numeric so averages can be calculated
        $addFields: {
            animal: {
                $toDouble: {$trim: {input: "$Daily_calorie_animal_protein"}}
            },
            vegetal: {
                $toDouble: {$trim: {input: "$Daily_calorie_vegetal_protein"}}
            },
            fat: {
                $toDouble: {$trim: {input: "$Daily_calorie_fat" }}
            },
            carbs: {
                $toDouble: {$trim: {input: "$Daily_calorie_carbohydrates"}}
            }
        }
    },
    { // Group by month and compute average calorie components
        $group: {
          _id: "$Month",
          avg_total_calories: {$avg: {$add: ["$animal", "$vegetal", "$fat", "$carbs"]}},
          avg_animal_protein: {$avg: "$animal"},
          avg_vegetal_protein: {$avg: "$vegetal"},
          avg_fat: {$avg: "$fat"},
          avg_carbs: {$avg: "$carbs"}
        }
    },
    { // Format final output fields
        $project: {
          _id: 0,
          Month: "$_id",
          avg_total_calories: "$avg_total_calories",
          avg_animal_protein: "$avg_animal_protein",
          avg_vegetal_protein: "$avg_vegetal_protein",
          avg_fat: "$avg_fat",
          avg_carbs: "$avg_carbs"
        }
    },
    { // Sort by highest average total calories to identify spike months
        $sort: {avg_total_calories: -1}
    }
])
// Feb to Apr top 3 (Spring)

// Identify healthier fast-food options to promote
// Beverage
db.mcdonaldata.aggregate([
    { // Filter only drinks categories
        $match: {
          menu: {$in: ["mccafe", "beverage"]}
        }
    },
    { // Clean sugar and calories fields for numeric comparison
        $addFields: {
            sugarNum: {
                $toDouble: {
                    $replaceAll: {
                        input: {
                            $replaceAll: {
                                input: "$sugar",
                                find: "Â",
                                replacement: ""
                            }
                        },
                        find: " ",
                        replacement: ""
                    }
                }
            },
            caloriesNum: {
                $toDouble: {
                    $replaceAll: {
                        input: {
                            $replaceAll: {
                                input: "$calories",
                                find: "Â",
                                replacement: ""
                                }
                            },
                        find: " ",
                        replacement: ""
                    }
                }
            }
        }
    },
    { // Keep only beverages with zero sugar
        $match: {
            sugarNum: 0
        }
    },
    { // Higher calorie zero sugar drinks appear at the top
        $sort: {caloriesNum: -1}
    },
    { // Format output, remove helper numeric fields
        $project: {
            _id: 0,
            sugarNum: 0,
            caloriesNum: 0
        }
    }
])
// Water, Coke Zero

// Main Meal
db.mcdonaldata.aggregate([
	{ // Only look at meal categories
		$match: {
			menu: {$in: ["regular", "breakfast", "gourmet"]}
		}
	},
	{ // Clean and convert nutrient fields for numeric filtering
		$addFields: {
			caloriesNum: {
				$convert: {
					input: {
						$replaceAll: {
							input: {
								$replaceAll: {
									input: {$trim: {input: "$calories"}},
									find: "Â",
									replacement: ""
								}
							},
							find: " ",
							replacement: ""
						}
					},
					to: "double",
					onError: null,
					onNull: null
				}
			},
			proteinNum: {
				$convert: {
					input: {
						$replaceAll: {
							input: {
								$replaceAll: {
									input: {$trim: {input: "$protien"}},
									find: "Â",
									replacement: ""
								}
							},
							find: " ",
							replacement: ""
						}
					},
					to: "double",
					onError: null,
					onNull: null
				}
			},
			totalfatNum: {
				$convert: {
					input: {
						$replaceAll: {
							input: {
								$replaceAll: {
									input: {$trim: {input: "$totalfat"}},
									find: "Â",
									replacement: ""
								}
							},
							find: " ",
							replacement: ""
						}
					},
					to: "double",
					onError: null,
					onNull: null
				}
			},
			sodiumNum: {
				$convert: {
					input: {
						$replaceAll: {
							input: {
								$replaceAll: {
									input: {$trim: {input: "$sodium"}},
									find: "Â",
									replacement: ""
								}
							},
							find: " ",
							replacement: ""
						}
					},
					to: "double",
					onError: null,
					onNull: null
				}
			},
			transfatNum: {
				$convert: {
					input: {
						$replaceAll: {
							input: {
								$replaceAll: {
									input: {$trim: {input: "$transfat"}},
									find: "Â",
									replacement: ""
								}
							},
							find: " ",
							replacement: ""
						}
					},
					to: "double",
					onError: null,
					onNull: null
				}
			}
		}
	},
	{ // Set healthy meal criteria
		$match: {
			caloriesNum: {$lte: 400},
			proteinNum: {$gt: 15},
			totalfatNum: {$lt: 20},
			sodiumNum: {$lt: 1000},
			transfatNum: {$lt: 10},
			item: {$not: {$regex: /Nugget|Fries|Wedges|Hash Brown|Fried/i}}
		}
	},
	{ // Sort by category and calories
		$sort: {
			menu: 1,
			caloriesNum: 1
		}
	},
	{ // Show original values, not numeric helper columns
		$project: {
			_id: 0,
			MyUnknownColumn: 1,
			item: 1,
			menu: 1,
			servesize: 1,
			calories: 1,
			protien: 1,
			totalfat: 1,
			transfat: 1,
			sodium: 1,
			satfat: 1,
			cholestrol: 1,
			carbs: 1,
			sugar: 1,
			addedsugar: 1
		}
	}
])
// 2 breakfast, 2 regulars
