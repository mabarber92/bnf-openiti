
import yaml
import os
import json
import pandas as pd
from scipy import stats

def fetch_config_paths(config_path = "./config.yml"):
    
    with open(config_path, "r") as f:
        config_data = yaml.load(f, Loader=yaml.FullLoader)
    
    outputs_path = config_data["pipeline_out_dir"]

    parsed_bnf = os.path.join(outputs_path, "bnf_parsed.json")
    matches_json = os.path.join(outputs_path, "matches", "full_7825", "matches_high_confidence.json")

    return parsed_bnf, matches_json

class OpenitiBnfMatches():
    """Class that takes a parsed BNF file and a matches file and uses them to
    create a data output or analyse the data"""
    def __init__ (self, parsed_bnf_json=None, matching_data_json=None):
        
        if parsed_bnf_json is None or matching_data_json is None:
            parsed_bnf, matches_json = fetch_config_paths()

        if parsed_bnf_json is None:
            parsed_bnf_json = parsed_bnf
        
        if matching_data_json is None:
            matching_data_json = matches_json

        self.bnf_data = self.load_json(parsed_bnf_json)["records"]
        self.matching_data = self.load_json(matches_json)

        # Process the matching data
        self._build_lookups()
        
    def load_json(self, json_path):
        with open(json_path, encoding='utf-8') as f:
            data = json.load(f)
        return data
    
    def _build_lookups(self):
        """Processing matches data to get a list of URIs, a dict of OpenITI - BNF IDs and a dict of BNF IDs to OpenITI matches
        To be used by later funcs"""
        self.matches_list = []
        self.openiti_dict = {}
        self.bnf_dict = {}
        for match in self.matching_data:
            matches = match["matches"]
            bnf_id = match["bnf_id"]
            self.matches_list.extend(matches)
            for uri in matches:
                self.openiti_dict.setdefault(uri, []).append(bnf_id)
                self.bnf_dict.setdefault(bnf_id, []).append(uri)

    def unique_count(self):
        """Get unique counts of OpenITI authors and books in the matches dataset"""
        books = list(set(self.matches_list))
        authors = list(set([b.split(".")[0] for b in books]))
        return len(books), len(authors)

    def _bin_chronology(self, bins_size=100):
        """Take list of URIs and return a dict of {period:count}"""
        period_dict = {}
        dates_list = [int(uri[:4]) for uri in self.matches_list]

        for i in range(0, 1400, bins_size):

            def dates_between(date):
                return i < date <= i + bins_size -1 
            
            filtered_dates = list(filter(dates_between, dates_list))
            period_dict[f"{i}-{i+bins_size-1}"] = len(filtered_dates)
        
        
        return period_dict

    def create_uri_df(self, append_fields=None, add_record_uri_counts=False):
        """Create a df where each row is a uri bnf record pair. append_fields specifies fields from
        bnf parsed to append to the df
        if add_record_uri_counts - add a row with number of uris the paired record has """
        list_for_df = []
        for uri, records in self.openiti_dict.items():
            for record in records:
                data_row = {"uri": uri,
                            "record": record}
                if append_fields is not None:
                    bnf_record = self.bnf_data[record]
                    for field in append_fields:
                        data_row[field] = bnf_record[field]
                if add_record_uri_counts:
                    uri_count = len(self.bnf_dict[record])
                    data_row["uris_for_record"] = uri_count
                list_for_df.append(data_row)
        
        return pd.DataFrame(list_for_df)


    def _uri_bnf_records_counts(self):
        """Create a df with counts of bnf records for each uri"""
        uri_records_df = self.create_uri_df()
        record_counts = uri_records_df.groupby(by="uri").count().reset_index()
        record_counts = record_counts.sort_values(by=["record"], ascending=False)
        return record_counts
    
    def calculate_outliers(self, count_df):
        count_df['z_score'] = stats.zscore(count_df["record"])
        outliers = count_df[abs(count_df['z_score']) > 3]
        return outliers

    def drop_lat_titles_containing(self, keyword):
        """Use keyword to drop records from the data where a certain keyword is contained exactly
        - primarily used to drop records that have exactly 'Coran.' in the data
        Note - that this permanently changes the data in the object and so removals will stack. 
        Would need to reload from json to get back to raw data"""
        
        excluded_records = []
        for record_id, data in self.bnf_data.items():
            for entry in data["title_lat"]:
                if keyword in entry:
                    excluded_records.append(record_id)
        
        # Remove records from matching data
        excluded_records = list(set(excluded_records))
        print(f"{len(excluded_records)} identified for exclusion with keyword: {keyword}")
        for matching_data in self.matching_data.copy():
            if matching_data["bnf_id"] in excluded_records:
                self.matching_data.remove(matching_data)
            
        # Recompute the lookups
        self._build_lookups()
        


    def run_summary_stats(self, summary_csv=None, id_outliers=False):
        """Run a series of summary statistics on the matches data"""

        # Get unique count
        book_count, author_count = self.unique_count()
        print("-"*10)
        print(f"Total unique books: {book_count}")
        print(f"Total unique authors: {author_count}")
        print("-"*10)

        # Print century spread of data
        date_counts = self._bin_chronology()
        print("Absolute URI counts by century:")
        for date_range, count in date_counts.items():
            print(f"{date_range}: {count}")
        print("-"*10)

        # Get count of BNF record per URI
        record_counts = self._uri_bnf_records_counts()
        if summary_csv is not None:
            record_counts.to_csv(summary_csv)
        print("Top URIS")
        print(record_counts.head(20))
        print("-"*10)
        
        # If id_outliers - check mathmatically for outlier
        if id_outliers:
            outliers = self.calculate_outliers(record_counts)
            print("Outliers:")
            print(outliers)
            print("-"*10)
    
    def create_records_csv(self, csv_path="uri_records.csv", 
                        append_fields=["gallica_url","title_lat", "creator_lat"],
                        remove_over=40, remove_outliers=False):
        """Output a csv where each row is a BNF URI pair"""
        # Get list of URIs that are over set remove_over limit or are outliers
        count_df = self._uri_bnf_records_counts()
        uris_over_limit = count_df[count_df["record"] > remove_over]["uri"].to_list()

        if remove_outliers:
            outliers = self.calculate_outliers(count_df)["uri"].to_list()
            uris_over_limit.extend(outliers)
        
        exclude_uris = list(set(uris_over_limit))
        full_df = self.create_uri_df(append_fields=append_fields, add_record_uri_counts=True)
        full_df = full_df[~full_df["uri"].isin(exclude_uris)]

        full_df.to_csv(csv_path, encoding='utf-8-sig', index=False)

if __name__ == "__main__":
    openiti_matches = OpenitiBnfMatches()
    openiti_matches.drop_lat_titles_containing("Coran.")
    openiti_matches.run_summary_stats(id_outliers=True)
    openiti_matches.create_records_csv()