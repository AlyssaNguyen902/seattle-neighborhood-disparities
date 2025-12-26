import os
import pandas as pd

disability_df =pd.read_csv("Project/Data/disability_health_insurance_cleaned.csv")
equity_df = pd.read_csv("Project/Data/Race_and_Social_Equity_cleaned.csv")

# Quick look of neighborhood names to see if they align
print(disability_df['neighborhood_name'].unique())
print(equity_df['l_hood'].unique())
print(equity_df['s_hood'].unique())

#Large amount of in Map Disability to Compist large neighborhoods (l_hood)
name_map = {
    "Alki/Admiral": "West Seattle",
    "Admiral": "West Seattle",
    "West Seattle Junction": "West Seattle",
    "Morgan Junction": "West Seattle",
    "Arbor Heights": "West Seattle",
    "Fauntleroy/Seaview": "West Seattle",
    "West Seattle Junction/Genesee Hill": "West Seattle",
    "North Delridge": "Delridge",
    "High Point": "Delridge",
    "Riverview": "Delridge",
    "Roxhill/Westwood": "Delridge",
    "Highland Park": "Delridge",
    "South Park": "Delridge",
    "Westwood-Highland Park": "Delridge",
    "Georgetown": "Greater Duwamish",
    "Greater Duwamish": "Greater Duwamish",
    "Duwamish/SODO": "Greater Duwamish",
    "South Beacon Hill/NewHolly": "Beacon Hill",
    "Beacon Hill": "Beacon Hill",
    "North Beacon Hill": "Beacon Hill",
    "North Beacon Hill/Jefferson Park": "Beacon Hill",
    "Rainier Beach": "Rainier Valley",
    "Columbia City": "Rainier Valley",
    "Mt. Baker/North Rainier": "Rainier Valley",
    "Othello": "Rainier Valley",
    "Mt Baker": "Rainier Valley",
    "Seward Park": "Seward Park",
    "Madrona/Leschi": "Central Area",
    "Judkins Park": "Central Area",
    "Central Area/Squire Park": "Central Area",
    "23 rd & Union": "Central Area",
    "23rd & Union-Jackson": "Central Area",
    "First Hill": "Downtown",
    "Belltown": "Downtown",
    "Downtown": "Downtown",
    "Downtown Commercial Core": "Downtown",
    "Pioneer Square/International District": "Downtown",
    "Capitol Hill": "Capitol Hill",
    "North Capitol Hill": "Capitol Hill",
    "Miller Park": "Capitol Hill",
    "Madison-Miller": "Capitol Hill",
    "Madison Park": "Capitol Hill",
    "First Hill/Capitol Hill": "Capitol Hill",
    "Montlake/Portage Bay": "Northeast",
    "Ravenna/Bryant": "Northeast",
    "Wedgwood/View Ridge": "Northeast",
    "Roosevelt": "Northeast",
    "Laurelhurst/Sand Point": "Northeast",
    "University District": "University District",
    "Aurora-Licton Springs": "Northgate",
    "Northgate/Maple Leaf": "Northgate",
    "Licton Springs": "Northgate",
    "Haller Lake": "Northgate",
    "Northgate": "Northgate",
    "Olympic Hills/Victory Heights": "Northgate",
    "Cedar Park/Meadowbrook": "Lake City",
    "Lake City": "Lake City",
    "Broadview/Bitter Lake": "Northwest",
    "Bitter Lake Village": "Northwest",
    "Crown Hill": "Northwest",
    "Greenwood-Phinney Ridge": "Northwest",
    "Greenwood/Phinney Ridge": "Northwest",
    "North Beach/Blue Ridge": "Northwest",
    "Green Lake": "North Central",
    "Fremont": "North Central",
    "Wallingford": "North Central",
    "Sunset Hill/Loyal Heights": "Ballard",
    "Whittier Heights": "Ballard",
    "Ballard": "Ballard",
    "Cascade/Eastlake": "Cascade",
    "Eastlake": "Cascade",
    "South Lake Union": "Cascade",
    "Magnolia": "Magnolia",
    "Interbay": "Interbay",
    "Queen Anne": "Queen Anne",
    "Uptown": "Queen Anne",
    "Lower Queen Anne": "Queen Anne",
    "Upper Queen Anne": "Queen Anne",
    "Outside Villages": None, #leave unmapped

    # Council districts drop
    "Council District 1": None,
    "Council District 2": None,
    "Council District 3": None,
    "Council District 4": None,
    "Council District 5": None,
    "Council District 6": None,
    "Council District 7": None,
}

# Apply the mapping to the 'neighborhood_name' column
disability_df['l_hood_mapped'] = disability_df['neighborhood_name'].map(name_map)

#checking missing
missing_mapped = disability_df[disability_df['l_hood_mapped'].isna()]
print("Unmapped Neighborhoods:", missing_mapped['neighborhood_name'])

#preview of map output
print(disability_df[['neighborhood_name', 'l_hood_mapped']].head(20))

#remove rows that could not be mapped
disability_df = disability_df[~disability_df['l_hood_mapped'].isna()].copy()
print("Rows Kept for joining:", len(disability_df))

#fix of the aggreate equity data so that there is only one row per large neighborhood
numeric_cols = [c for c in equity_df.columns if pd.api.types.is_numeric_dtype(equity_df[c])]

agg_map = {}
for col in numeric_cols:
    if col.endswith("_sum") or col.endswith("_count"):
        agg_map[col] = "sum"
    elif col.endswith("_mean"):
        agg_map[col] = "mean"
    else:
        agg_map[col] = "mean"

equity_df = equity_df.groupby("l_hood", as_index=False).agg(agg_map)

#merge datasets by large neighborhood
joined_df = disability_df.merge(
    equity_df,
    left_on='l_hood_mapped',
    right_on='l_hood',
    suffixes=('_disability', '_equity'),
)
print("Joined Data Preview:", joined_df.head())

unmatched = joined_df[joined_df['l_hood'].isna()]['l_hood_mapped'].unique()
print("Unmatched after join:" , unmatched)

# Save the joined dataset
joined_df.to_csv("Project/Data/disability_composite_joined.csv", index=False)