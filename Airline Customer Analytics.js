show dbs
use "local"
use "bc2402_gp"

/* Qn1 */
db.customer_support.aggregate([
    {
        $group: {
            _id: "$category",
            Flags: { $first: "$flags" },
            Instruction: { $first: "$instruction" },
            Category: { $first: "$category" },
            Intent: { $first: "$intent" },
            Response: { $first: "$response" }
        }
    }
]);
// Clearly shows that the flags without letter "B" are not part of the customer reviews

// Clean the data by removing the names which don't fit the category
db.customer_support.deleteMany({
    flags: { $not: /B/ }
});

// Check the categories after cleaning
db.customer_support.distinct("category");

// Count the number of categories after cleaning
db.customer_support.aggregate([
    {
        $group: { _id: "$category" }
    },
    {
        $count: "TotalCategories"
    }
]);


// Q2
db.customer_support.aggregate([
    {$match: {
            category: {$in: 
                ["ACCOUNT", "CANCEL", "CONTACT", "DELIVERY", "FEEDBACK", "INVOICE", "ORDER", "PAYMENT", "REFUND", "SHIPPING", "SUBSCRIPTION"]}
        }
    },
    {
        $group: {
            _id: "$category",
            "colloquial_only_count": {
                $sum: {
                    $cond: [
                        {$and: [
                                {$regexMatch: {input:"$flags", regex: "Q"}},
                                {$not: {$regexMatch: {input:"$flags", regex: "W"}}}
                            ]
                        }, 1, 0]
                }
            },
            "offensive_only_count": {
                $sum: {
                    $cond: [
                        {$and: [
                                {$regexMatch: {input:"$flags", regex: "W"}},
                                {$not: {$regexMatch: {input:"$flags", regex: "Q"}}}
                            ]
                        }, 1, 0]
                }
            },
            "both_colloquial_and_offensive_count": {
                $sum: {
                    $cond: [
                        {$and: [
                                {$regexMatch: {input:"$flags", regex: "Q"}},
                                {$regexMatch: {input:"$flags", regex: "W"}}
                            ]
                        }, 1, 0]
                }
            }
        }
    },
    {$project: {
            _id: 0,
            category: "$_id",
            "colloquial_only_count": 1,
            "offensive_only_count": 1,
            "both_colloquial_and_offensive_count": 1
        }
    }
])


/// Q3 ///
db.flight_delay.aggregate([
  {
    $match: {
      Cancelled: 1
    }
  },
  {
    $group: {
      _id: "$Airline",
      Instances: { $sum: 1 }
    }
  },
  {
    $project: {
      Airline: "$_id",
      Instances: 1,
      Type: { $literal: "Cancellation" }
    }
  },

  {
    $unionWith: {
      coll: "flight_delay",
      pipeline: [
        {
          $match: {
            $or: [
              { CarrierDelay: { $gt: 0 } },
              { WeatherDelay: { $gt: 0 } },
              { NASDelay: { $gt: 0 } },
              { SecurityDelay: { $gt: 0 } },
              { LateAircraftDelay: { $gt: 0 } }
            ]
          }
        },
        {
          $group: {
            _id: "$Airline",
            Instances: { $sum: 1 }
          }
        },
        {
          $project: {
            Airline: "$_id",
            Instances: 1,
            Type: { $literal: "Delay" }
          }
        }
      ]
    }
  },

  // Sort the results by Airline and Type for readability
  {
    $sort: { Airline: 1, Type: 1 }
  }
]);


// Q4
db.flight_delay.aggregate([
    {
        $match: {
            ArrDelay: { $gt: 0 }
        }
    },

    {
        $addFields: {
            YearMonth: { 
                $dateToString: { format: "%Y-%m", date: { $dateFromString: { dateString: "$Date", format: "%d-%m-%Y" } } }
            },
            Route: { $concat: ["$Origin", " to ", "$Dest"] }
        }
    },

    {
        $group: {
            _id: { YearMonth: "$YearMonth", Route: "$Route" },
            DelayCount: { $sum: 1 }
        }
    },

    {
        $sort: { "_id.YearMonth": 1, "DelayCount": -1 }
    },

    {
        $group: {
            _id: "$_id.YearMonth",  // Group by YearMonth
            Route: { $first: "$_id.Route" },  // Get the Route with the max DelayCount
            DelayCount: { $first: "$DelayCount" }  // Get the max DelayCount
        }
    },

    {
        $project: {
            YearMonth: "$_id",
            Route: 1,
            DelayCount: 1
        }
    },
    
    {
        $sort: { "YearMonth": 1 }
    }
]);


// Q5 
db.sia_stock.aggregate([
    // Step 1: Filter for the year 2023 and convert StockDate to ISODate
    {
        $addFields: {
            StockDate: { $dateFromString: { dateString: "$StockDate", format: "%m/%d/%Y" } }
        }
    },
    { $match: { StockDate: { $gte: ISODate("2023-01-01"), $lt: ISODate("2024-01-01") } } },
    
    // Step 2: Add fields for year and quarter
    {
        $addFields: {
            year: { $year: "$StockDate" },
            quarter: { $ceil: { $divide: [{ $month: "$StockDate" }, 3] } }
        }
    },
    
    // Step 3: Group by year and quarter to calculate metrics
    {
        $group: {
            _id: { year: "$year", quarter: "$quarter" },
            maxHigh: { $max: "$High" },
            minLow: { $min: "$Low" },
            avgPrice: { $avg: "$Price" }
        }
    },
    
    // Step 4: Sort by year and quarter
    { $sort: { "_id.year": 1, "_id.quarter": 1 } },
    
    // Step 5: Calculate QoQ changes using $setWindowFields
    {
        $setWindowFields: {
            sortBy: { "_id.year": 1, "_id.quarter": 1 },
            output: {
                prevMaxHigh: { $shift: { by: -1, output: "$maxHigh" } },
                prevMinLow: { $shift: { by: -1, output: "$minLow" } },
                prevAvgPrice: { $shift: { by: -1, output: "$avgPrice" } }
            }
        }
    },
    
    // Step 6: Calculate QoQ changes
    {
        $addFields: {
            qoqHighChange: { $subtract: ["$maxHigh", "$prevMaxHigh"] },
            qoqLowChange: { $subtract: ["$minLow", "$prevMinLow"] },
            qoqAvgChange: { $subtract: ["$avgPrice", "$prevAvgPrice"] }
        }
    },
    
    // Step 7: Project the final output
    {
        $project: {
            _id: 0,
            year: "$_id.year",
            quarter: "$_id.quarter",
            maxHigh: 1,
            minLow: 1,
            avgPrice: 1,
            qoqHighChange: 1,
            qoqLowChange: 1,
            qoqAvgChange: 1
        }
    }
]);



// Q6
/*Question 6: Additional Analysis has been done in a jupyter notebook, link has been provided in the report */
db.customer_booking.aggregate([
    
    // Step 1: Group by sales_channel and route, and compute the averages
    {
        $group: {
            _id: {
                sales_channel: "$sales_channel",
                route: "$route"
            },
            avgLengthOfStay: { $avg: "$length_of_stay" },
            avgWantsExtraBaggage: { $avg: "$wants_extra_baggage" },
            avgWantsPreferredSeat: { $avg: "$wants_preferred_seat" },
            avgWantsInFlightMeals: { $avg: "$wants_in_flight_meals" },
            avgFlightHour: { $avg: "$flight_hour" }
        }
    },
    
    // Step 2: Calculate the ratios
    {
        $project: {
            _id: 0,
            sales_channel: "$_id.sales_channel",
            route: "$_id.route",
            avgLengthOfStayPerAvgFlightHour: {
                $cond: [
                    { $eq: ["$avgFlightHour", 0] },
                    null,
                    { $divide: ["$avgLengthOfStay", "$avgFlightHour"] }
                ]
            },
            avgExtraBaggagePerAvgFlightHour: {
                $cond: [
                    { $eq: ["$avgFlightHour", 0] },
                    null,
                    { $divide: ["$avgWantsExtraBaggage", "$avgFlightHour"] }
                ]
            },
            avgPreferredSeatPerAvgFlightHour: {
                $cond: [
                    { $eq: ["$avgFlightHour", 0] },
                    null,
                    { $divide: ["$avgWantsPreferredSeat", "$avgFlightHour"] }
                ]
            },
            avgInFlightMealsPerAvgFlightHour: {
                $cond: [
                    { $eq: ["$avgFlightHour", 0] },
                    null,
                    { $divide: ["$avgWantsInFlightMeals", "$avgFlightHour"] }
                ]
            }
        }
    }
]);


// Q7 
//Q7
/*
[airlines_reviews] Airline seasonality. For each Airline and Class, display the averages of SeatComfort, FoodnBeverages, InflightEntertainment, ValueForMoney, and OverallRating for the seasonal and non-seasonal periods, respectively. 
*/

db.airlines_reviews.aggregate([
  {
    $addFields: {
      season: {
        $cond: {
          if: { $in: [{ $substr: ["$MonthFlown", 0, 3] }, ["Jun", "Jul", "Aug", "Sep"]] },
          then: "seasonal",
          else: "non-seasonal"
        }
      }
    }
  },
  {
    $group: {
      _id: {
        Airline: "$Airline",
        Class: "$Class",
        Season: "$season"
      },
      avgSeatComfort: { $avg: "$SeatComfort" },
      avgFoodnBeverages: { $avg: "$FoodnBeverages" },
      avgInflightEntertainment: { $avg: "$InflightEntertainment" },
      avgValueForMoney: { $avg: "$ValueForMoney" },
      avgOverallRating: { $avg: "$OverallRating" }
    }
  },
  {
    $sort: { "_id.Airline": 1, "_id.Class": 1, "_id.Season": 1 }
  }
])


// Q8 
db.airlines_reviews.aggregate([
    {$match: {
            Verified: "TRUE",
            Recommended: "no"
        }
    },
    {$group: {
            _id: null,
            TotalNoOfReviews: {$sum: 1}
        }
    }
])

// Count of Passengers for each Airline and TypeofTraveller
db.airlines_reviews.aggregate([
  {
    $match: {
      Verified: "TRUE",
      Recommended: "no"
    }
  },
  {
    $group: {
      _id: { Airline: "$Airline", TypeofTraveller: "$TypeofTraveller" },
      CountOfPassengers: { $sum: 1 }
    }
  },
  {
    $project: {
      _id: 0,
      Airline: "$_id.Airline",
      TypeOfTraveller: "$_id.TypeofTraveller",
      CountOfPassengers: 1
    }
  }
])


// Avg Overall Rating for each Airline
db.airlines_reviews.aggregate([
  {
    $match: {
      Verified: "TRUE",
      Recommended: "no"
    }
  },
  {
    $group: {
      _id: { Airline: "$Airline", TypeofTraveller: "$TypeofTraveller" },
      AvgOverallRating: { $avg: "$OverallRating" }
    }
  }
])


// Average rating for all categories 
db.airlines_reviews.aggregate([
  {
    $match: {
      Verified: "TRUE",
      Recommended: "no"
    }
  },
  {
    $group: {
      _id: { Airline: "$Airline", TypeofTraveller: "$TypeofTraveller" },
      AvgSeatComfort: { $avg: "$SeatComfort" },
      AvgStaffService: { $avg: "$StaffService" },
      AvgFoodBeverages: { $avg: "$FoodnBeverages" },
      AvgInflightEntertainment: { $avg: "$InflightEntertainment" },
      AvgValueForMoney: { $avg: "$ValueForMoney" }
    }
  }
])


// Count no. of reviewers that gave <=3 rating
db.airlines_reviews.aggregate([
  {
    $match: {
      Verified: "TRUE",
      Recommended: "no"
    }
  },
  {
    $group: {
      _id: { "Airline": "$Airline", "TypeofTraveller": "$TypeofTraveller" },
      SeatIssue: { $sum: { $cond: [{ $lte: ["$SeatComfort", 3] }, 1, 0] } },
      ServiceIssue: { $sum: { $cond: [{ $lte: ["$StaffService", 3] }, 1, 0] } },
      MealIssue: { $sum: { $cond: [{ $lte: ["$FoodnBeverages", 3] }, 1, 0] } },
      EntertainmentIssue: { $sum: { $cond: [{ $lte: ["$InflightEntertainment", 3] }, 1, 0] } },
      CostIssue: { $sum: { $cond: [{ $lte: ["$ValueForMoney", 3] }, 1, 0] } }
    }
  },
  {
    $project: {
      _id: 0, // Exclude `_id`
      "Airline": "$_id.Airline",
      "TypeofTraveller": "$_id.TypeofTraveller",
      "SeatIssue": 1,
      "ServiceIssue": 1,
      "MealIssue": 1,
      "EntertainmentIssue": 1,
      "CostIssue": 1
    }
  }
])


// Frequency of Words for SIA 
db.airlines_reviews.aggregate([
  {
    $match: {
      Airline: "Singapore Airlines"
    }
  },
  {
    $project: {
      TypeofTraveller: 1,
      Words: {
        $split: [
          { $toLower: "$Reviews" },
          " "
        ]
      }
    }
  },
  {
    $unwind: "$Words"
  },
  {
    $match: {
      Words: { $in: ["lost", "baggage", "delay", "uncomfortable", "legroom", "small", "leg room", "curt", "unfriendly", "rude", "dirty", "refund", "meal", "food", "wi-fi", "wifi", "console", "tv", "expensive"] }
    }
  },
  {
    $group: {
      _id: { TypeofTraveller: "$TypeofTraveller", Word: "$Words" },
      Frequency: { $sum: 1 }
    }
  },
  {
    $sort: { "_id.TypeofTraveller": 1, Frequency: -1 }
  }
])


// Frequency of Words for Qatar
db.airlines_reviews.aggregate([
  {
    $match: {
      Airline: "Qatar Airways"
    }
  },
  {
    $project: {
      TypeofTraveller: 1,
      Words: {
        $split: [
          { $toLower: "$Reviews" },
          " "
        ]
      }
    }
  },
  {
    $unwind: "$Words"
  },
  {
    $match: {
      Words: { $in: ["lost", "baggage", "delay", "uncomfortable", "legroom", "small", "leg room", "curt", "unfriendly", "rude", "dirty", "refund", "meal", "food", "wi-fi", "wifi", "console", "tv", "expensive"] }
    }
  },
  {
    $group: {
      _id: { TypeofTraveller: "$TypeofTraveller", Word: "$Words" },
      Frequency: { $sum: 1 }
    }
  },
  {
    $sort: { "_id.TypeofTraveller": 1, Frequency: -1 }
  }
])


// Q9 
db.airlines_reviews.aggregate([
    {
        // Stage 1: Filter for Singapore Airlines and Verified = "TRUE"
        $match: {
            Airline: "Singapore Airlines",
            Verified: "TRUE"
        }
    },
    {
        // Stage 2: Add a "Year" field by extracting the year from MonthFlown
        $addFields: {
            Year: { 
                $substr: ["$MonthFlown", { $subtract: [ { $strLenCP: "$MonthFlown" }, 2 ] }, 2]
            } // Extracts the last 2 characters based on string length
        }
    },
    {
        // Stage 3: Group by Year and calculate metrics
        $group: {
            _id: "$Year",
            TotalReviews: { $sum: 1 }, // Count total reviews
            YesCount: {
                $sum: { $cond: [{ $eq: ["$Recommended", "yes"] }, 1, 0] }
            }, // Count "yes" recommendations
            NoCount: {
                $sum: { $cond: [{ $eq: ["$Recommended", "no"] }, 1, 0] }
            }, // Count "no" recommendations
            AvgOverallRating: { $avg: "$OverallRating" }, // Average overall rating
            AvgSeatComfortRating: { $avg: "$SeatComfort" }, // Average seat comfort rating
            AvgStaffServiceRating: { $avg: "$StaffService" }, // Average staff service rating
            AvgFoodBeveragesRating: { $avg: "$FoodnBeverages" }, // Average food and beverage rating
            AvgEntertainmentRating: { $avg: "$InflightEntertainment" }, // Average entertainment rating
            AvgValueForMoneyRating: { $avg: "$ValueForMoney" } // Average value for money rating
        }
    },
    {
        // Stage 4: Add percentage calculations for recommendations
        $addFields: {
            YesPercent: { $multiply: [{ $divide: ["$YesCount", "$TotalReviews"] }, 100] },
            NoPercent: { $multiply: [{ $divide: ["$NoCount", "$TotalReviews"] }, 100] }
        }
    },
    {
        // Stage 5: Project the final fields for clean output
        $project: {
            Year: "$_id",
            TotalReviews: 1,
            YesCount: 1,
            NoCount: 1,
            YesPercent: 1,
            NoPercent: 1,
            AvgOverallRating: 1,
            AvgSeatComfortRating: 1,
            AvgStaffServiceRating: 1,
            AvgFoodBeveragesRating: 1,
            AvgEntertainmentRating: 1,
            AvgValueForMoneyRating: 1,
            _id: 0
        }
    },
    {
        // Stage 6: Sort by Year
        $sort: { Year: 1 }
    }
]);


// Q10 
db.airlines_reviews.aggregate([
  {
    $match: { Verified: "TRUE" }
  },
  {
    $group: {
      _id: null,
      seat_comfort: { $avg: "$SeatComfort" },
      staff_service: { $avg: "$StaffService" },
      food_and_beverages: { $avg: "$FoodnBeverages" },
      in_flight_entertainment: { $avg: "$InflightEntertainment" },
      value_for_money: { $avg: "$ValueForMoney" },
      overall_rating: { $avg: "$OverallRating" },
      yes_count: { $sum: { $cond: [{ $eq: ["$Recommended", "yes"] }, 1, 0] } },
      no_count: { $sum: { $cond: [{ $eq: ["$Recommended", "no"] }, 1, 0] } }
    }
  }
])

db.airlines_reviews.aggregate([
  {
    $match: {
      Verified: "TRUE",
      Airline: "Singapore Airlines",
      $or: [
        { Reviews: /safety/i },        // Case-insensitive search for "safety"
        { Reviews: /turbulence/i },    // Case-insensitive search for "turbulence"
        { Reviews: /compensation/i }   // Case-insensitive search for "compensation"
      ]
    }
  },
  {
    $project: {
      Reviews: 1,
      OverallRating: 1
    }
  }
])



// Extra Qn 1
db.sia_stock.aggregate([
    {
        $project: {
            year: { $year: { $dateFromString: { dateString: "$StockDate", format: "%m/%d/%Y" } } },
            month: { $month: { $dateFromString: { dateString: "$StockDate", format: "%m/%d/%Y" } } },
            High: { $toDouble: "$High" },
            Low: { $toDouble: "$Low" },
            Vol: {
                $cond: {
                    if: { $regexMatch: { input: "$Vol", regex: /M$/ } },
                    then: { $multiply: [{ $toDouble: { $substr: ["$Vol", 0, { $subtract: [{ $strLenCP: "$Vol" }, 1] }] } }, 1000000] },
                    else: {
                        $cond: {
                            if: { $regexMatch: { input: "$Vol", regex: /K$/ } },
                            then: { $multiply: [{ $toDouble: { $substr: ["$Vol", 0, { $subtract: [{ $strLenCP: "$Vol" }, 1] }] } }, 1000] },
                            else: { $toDouble: "$Vol" }
                        }
                    }
                }
            }
        }
    },
    {
        $group: {
            _id: { year: "$year", month: "$month" },
            highPrice: { $max: "$High" },
            lowPrice: { $min: "$Low" },
            totalVolume: { $sum: "$Vol" },
            dailyVolumeAvg: { $avg: "$Vol" }
        }
    },
    {
        $project: {
            year: "$_id.year",
            month: "$_id.month",
            highPrice: 1,
            lowPrice: 1,
            totalVolume: 1,
            dailyVolumeAvg: 1,
            _id: 0
        }
    },
    { $sort: { year: 1, month: 1 } }
]);


// Extra Qn 2
db.customer_booking.find().pretty();

[{ UniqueRoutes: db.customer_booking.distinct("route").length }]
[{ UniqueBookingOrigins: db.customer_booking.distinct("booking_origin").length }]
[{ UniqueTripTypes: db.customer_booking.distinct("trip_type") }]
[{ UniqueSalesChannels: db.customer_booking.distinct("sales_channel") }]
[{ UniqueFlightHours: db.customer_booking.distinct("flight_hour").length }]
[{ UniqueFlightDays: db.customer_booking.distinct("flight_day").length }]
[{ UniqueNumPassengers: db.customer_booking.distinct("num_passengers").length }]

db.customer_booking.aggregate([
  {
    $group: {
      _id: null,
      MaxNumPassengers: { $max: "$num_passengers" },
      MinNumPassengers: { $min: "$num_passengers" },
      AvgNumPassengers: { $avg: "$num_passengers" }
    }
  },
  {
    $project: {
      MaxNumPassengers: 1,
      MinNumPassengers: 1,
      AvgNumPassengers: 1
    }
  }
]);

db.customer_booking.aggregate([
  {
    $group: {
      _id: null,
      MaxPurchaseLead: { $max: "$purchase_lead" },
      MinPurchaseLead: { $min: "$purchase_lead" },
      AvgPurchaseLead: { $avg: "$purchase_lead" }
    }
  },
  {
    $project: {
      MaxPurchaseLead: 1,
      MinPurchaseLead: 1,
      AvgPurchaseLead: 1
    }
  }
]);

db.customer_booking.aggregate([
  {
    $group: {
      _id: null,
      MaxLengthOfStay: { $max: "$length_of_stay" },
      MinLengthOfStay: { $min: "$length_of_stay" },
      AvgLengthOfStay: { $avg: "$length_of_stay" }
    }
  },
  {
    $project: {
      MaxLengthOfStay: 1,
      MinLengthOfStay: 1,
      AvgLengthOfStay: 1
    }
  }
]);

db.customer_booking.aggregate([
  {
    $group: {
      _id: null,
      MaxFlightDuration: { $max: "$flight_duration" },
      MinFlightDuration: { $min: "$flight_duration" },
      AvgFlightDuration: { $avg: "$flight_duration" }
    }
  },
  {
    $project: {
      MaxFlightDuration: 1,
      MinFlightDuration: 1,
      AvgFlightDuration: 1
    }
  }
]);

db.customer_booking.aggregate([
  {
    $group: {
      _id: "$route",
      BookingCount: { $sum: 1 }
    }
  },
  { $sort: { BookingCount: -1 } },
  { $limit: 10 },
  {
    $project: {
      Route: "$_id",
      BookingCount: 1,
      _id: 0
    }
  }
]);

db.customer_booking.aggregate([
  {
    $group: {
      _id: "$booking_origin",
      BookingCount: { $sum: 1 }
    }
  },
  { $sort: { BookingCount: -1 } },
  { $limit: 10 },
  {
    $project: {
      BookingOrigin: "$_id",
      BookingCount: 1,
      _id: 0
    }
  }
]);

/* Step 1b: Understanding airlines_reviews individually */
db.airlines_reviews.find().pretty();

[{ TotalReviews: db.airlines_reviews.countDocuments() }]
[{ UniqueReviewers: db.airlines_reviews.distinct("Name").length }]

db.airlines_reviews.aggregate([
  {
    $group: {
      _id: null,
      EarliestReviewDate: { $min: "$ReviewDate" },
      LatestReviewDate: { $max: "$ReviewDate" }
    }
  },
  {
    $project: {
      EarliestReviewDate: 1,
      LatestReviewDate: 1
    }
  }
]);

[{ UniqueAirlines: db.airlines_reviews.distinct("Airline") }]

db.airlines_reviews.aggregate([
  {
    $group: {
      _id: "$Verified",
      Count: { $sum: 1 }
    }
  },
  {
    $project: {
      Verified: "$_id",
      Count: 1,
      _id: 0
    }
  }
]);

[{ TypesOfTraveller: db.airlines_reviews.distinct("TypeofTraveller") }]

db.airlines_reviews.aggregate([
  {
    $group: {
      _id: null,
      EarliestMonthFlown: { $min: "$MonthFlown" },
      LatestMonthFlown: { $max: "$MonthFlown" }
    }
  },
  {
    $project: {
      EarliestMonthFlown: 1,
      LatestMonthFlown: 1
    }
  }
]);

[{ UniqueRoutes: db.airlines_reviews.distinct("Route").length }]

[{ UniqueClass: db.airlines_reviews.distinct("Class") }]

db.airlines_reviews.aggregate([
  {
    $group: {
      _id: "$Recommended",
      Count: { $sum: 1 }
    }
  },
  {
    $project: {
      Recommendation: "$_id",
      Count: 1,
      _id: 0
    }
  }
]);

db.airlines_reviews.aggregate([
  {
    $group: {
      _id: { airline: "$Airline", class: "$Class", traveler: "$TypeofTraveller" },
      MinSeatComfort: { $min: "$SeatComfort" },
      MaxSeatComfort: { $max: "$SeatComfort" },
      AvgSeatComfort: { $avg: "$SeatComfort" },
      MinStaffService: { $min: "$StaffService" },
      MaxStaffService: { $max: "$StaffService" },
      AvgStaffService: { $avg: "$StaffService" },
      MinFoodnBeverages: { $min: "$FoodnBeverages" },
      MaxFoodnBeverages: { $max: "$FoodnBeverages" },
      AvgFoodnBeverages: { $avg: "$FoodnBeverages" },
      MinInflightEntertainment: { $min: "$InflightEntertainment" },
      MaxInflightEntertainment: { $max: "$InflightEntertainment" },
      AvgInflightEntertainment: { $avg: "$InflightEntertainment" },
      MinValueForMoney: { $min: "$ValueForMoney" },
      MaxValueForMoney: { $max: "$ValueForMoney" },
      AvgValueForMoney: { $avg: "$ValueForMoney" },
      MinOverallRating: { $min: "$OverallRating" },
      MaxOverallRating: { $max: "$OverallRating" },
      AvgOverallRating: { $avg: "$OverallRating" }
    }
  },
  {
    $project: {
      Airline: "$_id.airline",
      Class: "$_id.class",
      TypeofTraveller: "$_id.traveler",
      MinSeatComfort: 1,
      MaxSeatComfort: 1,
      AvgSeatComfort: 1,
      MinStaffService: 1,
      MaxStaffService: 1,
      AvgStaffService: 1,
      MinFoodnBeverages: 1,
      MaxFoodnBeverages: 1,
      AvgFoodnBeverages: 1,
      MinInflightEntertainment: 1,
      MaxInflightEntertainment: 1,
      AvgInflightEntertainment: 1,
      MinValueForMoney: 1,
      MaxValueForMoney: 1,
      AvgValueForMoney: 1,
      MinOverallRating: 1,
      MaxOverallRating: 1,
      AvgOverallRating: 1,
      _id: 0
    }
  },
  { $sort: { Airline: 1, Class: 1, TypeofTraveller: 1 } }
]);

db.airlines_reviews.aggregate([
  {
    $group: {
      _id: { recommended: "$Recommended", airline: "$Airline", class: "$Class", traveler: "$TypeofTraveller" },
      MinOverallRating: { $min: "$OverallRating" },
      MaxOverallRating: { $max: "$OverallRating" }
    }
  },
  {
    $project: {
      Airline: "$_id.airline",
      Recommendation: "$_id.recommended",
      Class: "$_id.class",
      TypeofTraveller: "$_id.traveler",
      MinOverallRating: 1,
      MaxOverallRating: 1,
      _id: 0
    }
  },
  { $sort: { Airline: 1, Recommendation: -1, Class: 1, TypeofTraveller: 1 } }
]);


/* Step 3b: Building the NoSQL Code*/
// Pre-Cursor: Showing Steps to Building Sentiment Dictionary
const stopWords = [
  "a", "about", "above", "after", "again", "against", "all", "am", "an", "and", "any", "are", "aren", "aren't",
  "as", "at", "be", "because", "been", "before", "being", "below", "between", "both", "but", "by", "can",
  "can't", "cannot", "could", "couldn't", "did", "didn't", "do", "does", "doesn't", "doing", "don't", "down",
  "during", "each", "few", "for", "from", "further", "had", "hadn't", "has", "hasn't", "have", "haven't", "having",
  "he", "he's", "her", "here", "here's", "hers", "herself", "him", "himself", "his", "how", "how's", "i", "i'm", "i've",
  "i'll", "i'd", "if", "in", "into", "is", "isn't", "it", "it's", "its", "itself", "let", "let's", "me", "more", "most",
  "much", "must", "my", "myself", "no", "nor", "not", "of", "off", "on", "once", "only", "or", "other", "ought", "our",
  "ours", "ourselves", "out", "over", "own", "same", "she", "she's", "should", "shouldn't", "so", "some", "such",
  "than", "that", "that's", "the", "their", "theirs", "them", "themselves", "then", "there", "there's", "these", "they",
  "they're", "this", "those", "through", "to", "too", "under", "until", "up", "very", "was", "wasn't", "we", "we're",
  "were", "weren't", "what", "what's", "when", "when's", "where", "where's", "which", "while", "who", "who's", "whom",
  "why", "why's", "with", "would", "you", "you'd", "you'll", "you're", "you've", "your", "yours", "yourself", "yourselves"
];

// Sub-step 0.1: Tokenize and Filter
db.airlines_reviews.aggregate([
  {
    $project: {
      review_id: "$_id",
      words: {
        $filter: {
          input: {
            $map: {
              input: { $regexFindAll: { input: "$Reviews", regex: "\\w+" } },
              as: "word",
              in: { $toLower: "$$word.match" }
            }
          },
          as: "word",
          cond: {
            $and: [
              { $ne: ["$$word", null] }, 
              { $not: { $in: ["$$word", stopWords] } }
            ]
          }
        }
      }
    }
  },
  { $unwind: "$words" },
  {
    $group: {
      _id: "$words",
      frequency: { $sum: 1 }
    }
  },
  { $match: { frequency: { $gte: 236 } } },
  { $sort: { frequency: -1 } },
  { $limit: 15 } // Limit the output to 15 documents so it doesn't print out everything
]).forEach(printjson);

// Sub-step 0.2: Simulate Sentiment Dictionary Creation
db.airlines_reviews.aggregate([
  {
    $project: {
      review_id: "$_id",
      words: {
        $filter: {
          input: {
            $map: {
              input: { $regexFindAll: { input: "$Reviews", regex: "\\w+" } },
              as: "word",
              in: { $toLower: "$$word.match" }
            }
          },
          as: "word",
          cond: {
            $and: [
              { $ne: ["$$word", null] },
              { $not: { $in: ["$$word", stopWords] } }
            ]
          }
        }
      }
    }
  },
  { $unwind: "$words" },
  {
    $group: {
      _id: "$words",
      frequency: { $sum: 1 }
    }
  },
  {
    $addFields: {
      sentiment: {
        $cond: {
          if: { $in: ["$_id", ["amazing", "excellent", "good", "great", "impressive", "committed", "passing"]] }, // Small sample of positive words
          then: "positive",
          else: {
            $cond: {
              if: { $in: ["$_id", ["bad", "poor", "terrible", "disappointing", "unpleasant", "embarassing", "indifferent", "regret"]] }, // Small Sample of negative words
              then: "negative",
              else: "neutral"
            }
          }
        }
      }
    }
  },
  { $limit: 15 } // Limit the output to 15 documents so it doesn't print out everything
]).forEach(printjson);


// Sub-Step 1: Add 'Type of Traveller' field to customer_booking
db.customer_booking.aggregate([
  {
    $addFields: {
      TypeofTraveller: {
        $switch: {
          branches: [
            {
              case: { $and: [{ $eq: ["$num_passengers", 1] }, { $lte: ["$length_of_stay", 7] }] },
              then: "Business"
            },
            {
              case: { $and: [{ $eq: ["$num_passengers", 1] }, { $gt: ["$length_of_stay", 7] }] },
              then: "Solo Leisure"
            },
            { case: { $eq: ["$num_passengers", 2] }, then: "Couple Leisure" },
            { case: { $gte: ["$num_passengers", 3] }, then: "Family Leisure" }
          ],
          default: null // Fallback for unexpected cases
        }
      }
    }
  },
  {
    $merge: {
      into: "customer_booking",
      whenMatched: "merge",
      whenNotMatched: "insert"
    }
  }
]);


// Sub-step 2: Standardize Routes in airlines_reviews and Preserve Original Route
db.airlines_reviews.aggregate([
  {
    $project: {
      _id: 1,
      Route: 1,
      original_route: "$Route",
      preprocessed_route: {
        $reduce: {
          input: { $split: [{ $replaceAll: { input: "$Route", find: "via", replacement: "" } }, "to"] },
          initialValue: [],
          in: {
            $concatArrays: [
              "$$value",
              [{ $trim: { input: "$$this" } }]
            ]
          }
        }
      },
      Title: 1,
      Reviews: 1,
      TypeofTraveller: 1,
      Class: 1,
      SeatComfort: 1,
      StaffService: 1,
      FoodnBeverages: 1,
      InflightEntertainment: 1,
      ValueForMoney: 1,
      OverallRating: 1,
      SentimentScore: 1
    }
  },

  {
    $lookup: {
      from: "airport_codes",
      let: { route_segments: "$preprocessed_route" },
      pipeline: [
        {
          $match: {
            $expr: {
              $in: ["$Code", "$$route_segments"]
            }
          }
        }
      ],
      as: "matched_airports"
    }
  },
  
  {
    $addFields: {
      unique_airport_codes: {
        $reduce: {
          input: "$matched_airports.Code",
          initialValue: [],
          in: {
            $cond: [
              { $in: ["$$this", "$$value"] },
              "$$value",
              { $concatArrays: ["$$value", ["$$this"]] }
            ]
          }
        }
      }
    }
  },

  {
    $addFields: {
      StandardizedRoute: {
        $cond: [
          { $gte: [{ $size: "$unique_airport_codes" }, 2] },
          {
            $reduce: {
              input: { $sortArray: { input: "$unique_airport_codes", sortBy: 1 } },
              initialValue: "",
              in: { $concat: ["$$value", "$$this"] }
            }
          },
          null
        ]
      }
    }
  },

  {
    $match: {
      StandardizedRoute: { $ne: null }
    }
  },

  {
    $project: {
      _id: 1,
      Route: 1,
      StandardizedRoute: 1,
      Title: 1,
      Reviews: 1,
      TypeofTraveller: 1,
      Class: 1,
      SeatComfort: 1,
      StaffService: 1,
      FoodnBeverages: 1,
      InflightEntertainment: 1,
      ValueForMoney: 1,
      OverallRating: 1,
      SentimentScore: 1
    }
  },

  {
    $merge: {
      into: "airlines_reviews",
      whenMatched: "merge",
      whenNotMatched: "discard"
    }
  }
]);


// Sub-Step 3 Develop new Consolidated_Data show collections
db.customer_booking.aggregate([
  {
    $addFields: {
      TypeofTraveller: {
        $switch: {
          branches: [
            { case: { $and: [{ $eq: ["$num_passengers", 1] }, { $lte: ["$length_of_stay", 7] }] }, then: "Business" },
            { case: { $and: [{ $eq: ["$num_passengers", 1] }, { $gt: ["$length_of_stay", 7] }] }, then: "Solo Leisure" },
            { case: { $eq: ["$num_passengers", 2] }, then: "Couple Leisure" },
            { case: { $gte: ["$num_passengers", 3] }, then: "Family Leisure" }
          ],
          default: null
        }
      }
    }
  },

  {
    $lookup: {
      from: "airlines_reviews",
      let: { standardized_route: "$Route", traveler_type: "$TypeofTraveller" },
      pipeline: [
        {
          $match: {
            $expr: {
              $and: [
                { $eq: ["$StandardizedRoute", "$$standardized_route"] },
                { $eq: ["$TypeofTraveller", "$$traveler_type"] }
              ]
            }
          }
        },
        {
          $group: {
            _id: null,
            AvgSeatComfort: { $avg: "$SeatComfort" },
            AvgStaffService: { $avg: "$StaffService" },
            AvgFoodnBeverages: { $avg: "$FoodnBeverages" },
            AvgInflightEntertainment: { $avg: "$InflightEntertainment" },
            AvgValueForMoney: { $avg: "$ValueForMoney" },
            AvgOverallRating: { $avg: "$OverallRating" },
            TotalReviews: { $sum: 1 }
          }
        }
      ],
      as: "RelevantReviews"
    }
  },

  {
    $match: {
      "RelevantReviews.0": { $exists: true }
    }
  },

  {
    $addFields: {
      RelevantReviews: {
        AverageSeatComfort: { $arrayElemAt: ["$RelevantReviews.AvgSeatComfort", 0] },
        AverageStaffService: { $arrayElemAt: ["$RelevantReviews.AvgStaffService", 0] },
        AverageFoodnBeverages: { $arrayElemAt: ["$RelevantReviews.AvgFoodnBeverages", 0] },
        AverageInflightEntertainment: { $arrayElemAt: ["$RelevantReviews.AvgInflightEntertainment", 0] },
        AverageValueForMoney: { $arrayElemAt: ["$RelevantReviews.AvgValueForMoney", 0] },
        AverageOverallRating: { $arrayElemAt: ["$RelevantReviews.AvgOverallRating", 0] },
        TotalReviews: { $arrayElemAt: ["$RelevantReviews.TotalReviews", 0] }
      }
    }
  },

  {
    $project: {
      RelevantReviews: {
        AverageSeatComfort: 1,
        AverageStaffService: 1,
        AverageFoodnBeverages: 1,
        AverageInflightEntertainment: 1,
        AverageValueForMoney: 1,
        AverageOverallRating: 1,
        TotalReviews: 1
      },
      route: 1,
      TypeofTraveller: 1,
      Flight_Hour: 1,
      Flight_Duration: 1,
      Purchase_Lead: 1,
      num_passengers: 1,
      length_of_stay: 1,
      sales_channel: 1,
      booking_origin: 1
    }
  },

  { $out: "Consolidated_Data" }
]);

print("Consolidated_Data Collection:");
db.Consolidated_Data.find().limit(15).pretty();