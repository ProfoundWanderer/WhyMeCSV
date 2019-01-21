import pandas as pd
from header_list import rename_list, match_list
import time


def main():
    # point to file location.
    filename = "/Users/derrick/Documents/Random Stuff/WhyMeCSV/test-csvs/name2long.csv"
    # sep=None so pandas tries to get the delimiter and dtype=str so columns don't sometimes have .0 added
    df = pd.read_csv(filename, dtype=str, encoding="ISO-8859-1")
    df.columns = [
        i.lower().replace(" ", "_") for i in df.columns
    ]  # lower case and replace spaces
    # removes empty rows then empty columns
    df = df.dropna(how="all")
    df = df.dropna(axis=1, how="all")

    # if first_name and last_name not in file then it tries to see if there is something like name then split it into
    # first_name and last_name
    df = try_creating_first_and_last_name(df)

    # trying to change the headers of the file to ones that will automatically match with ones in the system
    df = match_column_headers(df)

    # moves values in first_name column that are more than 256 characters (that is the limit for the bulk import tool)
    # to the long_first_name column so it is not rejected
    if (df["first_name"].str.len() > 256).any():
        df = move_long_names(df, "first_name")
    if (df["last_name"].str.len() > 256).any():
        df = move_long_names(df, "last_name")

    # tries to join things to make up address if it is not already a column in the file
    if "address" not in df.columns:
        df = try_creating_address(df)

    # tries to join things to make up assigned_agent if it is not already a column in the file
    if "assigned_agent" not in df.columns:
        df = try_creating_assigned_agent(df)

    # tries to join things to make up second_contact_name if it is not already a column in the file
    if "second_contact_name" not in df.columns:
        df = try_creating_second_contact_name(df)

    # if email is a column in the file it attempts to clean up the column by keeping only one email per row and moving
    # others to another column. It also tries to move "invalid" emails.
    if "email" in df.columns:
        df = clean_email_column(df)

    # does similar to clean_email_column
    if "phone" in df.columns:
        df = clean_phone_column(df)

    # merges rows with the same email then merges rows with the same contact_id
    # sending two things only works if the other column doesn't have an email if it does this the email just vanishes
    if "email" in df.columns:
        df = merge_rows(df, "email")

    if "phone" in df.columns:
        df = clean_phone_column(df)

    df = cleanup(df)

    # Convert header names back so system automatically catches it
    df.columns = [i.title().replace("_", " ") for i in df.columns]
    df.to_csv("/Users/derrick/Desktop/done.csv", index=False)


def try_creating_first_and_last_name(df):
    if "first_name" not in df.columns or "last_name" not in df.columns:
        # for liondesk
        if "name" in df.columns:
            if "last_name" in df.columns:
                df[["first_name", "last_name"]] = df["name"].str.split(
                    " ", 1, expand=True
                )
            elif "last_name" not in df.columns:
                df[["first_name", "last_name"]] = df["name"].str.split(
                    " ", 1, expand=True
                )
        # for top producer
        elif "contact" in df.columns:
            df[["last_name", "first_name"]] = df["contact"].str.split(
                ",", 1, expand=True
            )

    return df


def match_column_headers(df):
    """
       This is trying to match the headers of the column to one of the options in rename_list so they will be
       automatically matched by the system when uploaded. The commented out chunk that starts with
       `if rename_col not in df.columns and i < 4:` was there originally due to needed certain columns (first_name,
       last_name, email, phone) for the merger to work so this part tried to guess something close to these columns but
       since those aren't needed for the merger anymore this part isn't needed. I left it in just in case I decided
       guessing column names may be useful.
       """
    # for each header column name in rename_list
    for i, rename_col in enumerate(rename_list):
        # get  i  nested list in match_list (the list with all possible column names) and assign it to current_list
        current_list = match_list[i]
        # for each possible column name in current_list
        for try_col in current_list:
            # if the rename_col not in df
            if rename_col not in df.columns:
                try:
                    # try to find try_col in df and rename it to what rename_col is
                    df.rename(columns={try_col: rename_col}, inplace=True)
                    """
                    if the rename does not add rename_col to df and i is less than 4 then do the below code
                    I have i < 4 because the first 4 columns (which were first_name, last_name, email, phone) was 
                    needed for the merger so they go through an additional matching attempt by basically trying to 
                    find something close to the column names.
                    """
                    """
                    # create empty list of tried column header list from current_list. **If using this section of code
                    # add tried_colname before `for try_col`
                    # tried_colname = []

                    # add try_col to tried_col list so we don't try it again
                    tried_colname.append(try_col)

                    if rename_col not in df.columns and i < 4:
                        # if the number of items we tried equals the number of items in the list
                        if len(tried_colname) == len(current_list):
                            try:
                                # try to find the first column in df that is similar to rename_col 
                                # and rename it to whatever rename_col is
                                df = df.rename(columns={df.filter(like=rename_col).columns[0]: rename_col})
                                # print('Filter match', rename_col)
                                continue
                            # if try didn't work then throw exception since those 4 columns are needed for merger
                            except Exception as e:
                                # print(f"Unable to match a col the same as or close to {rename_col}. - Exception: {e}")
                                break
                        else:
                            continue
                    # this breaks so it doesn't continue trying to check when it has already been match
                    """
                    if rename_col in df.columns:
                        # print(f"Matched {rename_col} with {try_col}.")
                        break
                    else:
                        # print(f"Unable to match {rename_col} with {try_col}.")
                        continue
                except Exception as e:
                    print(f"How did you get here!? - Exception: {e}")
                    break

    return df


def move_long_names(df, type_of_name):
    # moves names over 256 characters to new column
    df[f"long_{type_of_name}"] = df[df[type_of_name].str.len() > 256][type_of_name]
    # only keeps values in the first_name column if it is less than or equal to 256 characters
    df[type_of_name] = df[df[type_of_name].str.len() <= 256][type_of_name]

    return df


def try_creating_address(df):
    # Got rid of astype(str) before .fillna('') and it resolved the random nan showing up in the address field
    # if all of these things are columns in the df then combine them under the column name 'address'
    if {
        "house_number",
        "dir_prefix",
        "street",
        "street_type",
        "dir_suffix",
        "suite",
        "po_box",
    }.issubset(df.columns):
        df["address"] = (
                df["house_number"].fillna("")
                + " "
                + df["dir_prefix"].fillna("")
                + " "
                + df["street"].fillna("")
                + " "
                + df["street_type"].fillna("")
                + " "
                + df["dir_suffix"].fillna("")
                + " "
                + df["suite"].fillna("")
                + " "
                + df["po_box"].fillna("")
        )
    elif {
        "house_number",
        "direction_prefix",
        "street",
        "street_designator",
        "suite_no",
    }.issubset(df.columns):
        df["address"] = (
                df["house_number"].fillna("")
                + " "
                + df["direction_prefix"].fillna("")
                + " "
                + df["street"].fillna("")
                + " "
                + df["street_designator"].fillna("")
                + " "
                + df["suite_no"].fillna("")
        )
    else:
        pass

    return df


def try_creating_assigned_agent(df):
    # if these columns in the df then combine them under the column name 'assigned_agent'
    if {"member_first_name", "member_last_name"}.issubset(df.columns):
        df["assigned_agent"] = (
                df["member_first_name"].fillna("") + " " + df["member_last_name"].fillna("")
        )
    else:
        pass

    return df


def try_creating_second_contact_name(df):
    # if all of these things are columns in the df then combine them under the column name 'second_contact_name'
    if {
        "secondary_title",
        "secondary_first_name",
        "secondary_nickname",
        "secondary_last_name",
    }.issubset(df.columns):
        df["second_contact_name"] = (
                df["secondary_title"].fillna("")
                + " "
                + df["secondary_first_name"].fillna("")
                + " "
                + df["secondary_nickname"].fillna("")
                + df["secondary_last_name"].fillna("")
        )
    elif {"first_name_2", "last_name_2"}.issubset(df.columns):
        df["second_contact_name"] = (
                df["first_name_2"].fillna("") + " " + df["last_name_2"].fillna("")
        )
    else:
        pass

    return df


def clean_email_column(df):
    # makes contents of email column lower case so it doesn't think example@Yahoo.com
    # and example@yahoo.com are different emails
    df["email"] = df.email.astype(str).str.lower()

    if (df["email"].str.contains(",")).any():
        df["secondary_email"] = None
        df.columns = df.columns.fillna("secondary_email")

        # splits email list so that everything before comma is put in email and the rest into secondary_email
        df["email"], df["secondary_email"] = df["email"].str.split(",", 1).str

        if (
                "second_contact_email" in df.columns
        ):  # this handles in case they do have the 'second_contact_email' column
            # Merges secondary_email into the 'second_contact_email' column
            df["second_contact_email"] = (
                    df["second_contact_email"].fillna("")
                    + ", "
                    + df["secondary_email"].fillna("")
            )
            # this is to just clean up column (i.e. remove leadning what space and random extra commas from merge)
            df["second_contact_email"] = df["second_contact_email"].replace(
                to_replace=r"((, )$|[,]$)|(^\s)", value="", regex=True
            )

            df = df.iloc[
                 :, :-1
                 ]  # drops last column so there isn't double 'second_contact_email'

        df.rename(columns={"secondary_email": "second_contact_email"}, inplace=True)
    else:
        pass

    # if there is a bad email then do stuff. its here to help with speed (not an issue but who knows)
    # and to stop adding a second_contact_email when its not needed
    if ~df.email.str.contains(r"^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-]+$").all():
        if "second_contact_email" in df.columns:
            # validate email and move bad ones
            df["temp_second_contact_email"] = df[
                ~df["email"].str.contains(
                    pat=r"^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-]+$",
                    case=False,
                    na=False,
                )
            ]["email"]
            df["email"] = df[
                df["email"].str.contains(
                    pat=r"^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-]+$",
                    case=False,
                    na=False,
                )
            ]["email"]
            # merges columns so original second_contact_email doesn't get replaced by temp_second_contact_email
            df["second_contact_email"] = (
                    df["second_contact_email"].fillna("")
                    + ", "
                    + df["temp_second_contact_email"].fillna("")
            )
            del df["temp_second_contact_email"]
            # this is to just clean up column (i.e. remove leadning what space and random extra commas from merge)
            df["second_contact_email"] = df["second_contact_email"].replace(
                to_replace=r"((, )$|[,]$)|(^\s)", value="", regex=True
            )
            # definitely not needed but one case bothered me so I added it
            df["second_contact_email"] = df["second_contact_email"].replace(
                to_replace=r"(  )", value=" ", regex=True
            )
        else:
            if "second_contact_email" not in df.columns:
                df["second_contact_email"] = ""
                # validate email and move bad ones
                df["temp_second_contact_email"] = df[
                    ~df["email"].str.contains(
                        pat=r"^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-]+$",
                        case=False,
                        na=False,
                    )
                ]["email"]
                df["email"] = df[
                    df["email"].str.contains(
                        pat=r"^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-]+$",
                        case=False,
                        na=False,
                    )
                ]["email"]
                # merges columns so original second_contact_email doesn't get replaced by temp_second_contact_email
                df["second_contact_email"] = (
                        df["second_contact_email"].fillna("")
                        + ", "
                        + df["temp_second_contact_email"].fillna("")
                )
                del df["temp_second_contact_email"]
                # this is to just clean up column (i.e. remove leadning what space and random extra commas from merge)
                df["second_contact_email"] = df["second_contact_email"].replace(
                    to_replace=r"((, )$|[,]$)|(^\s)", value="", regex=True
                )
                # definitely not needed but one case bothered me so I added it
                df["second_contact_email"] = df["second_contact_email"].replace(
                    to_replace=r"(  )", value=" ", regex=True
                )

    return df


def clean_phone_column(df):
    if df.phone.astype(str).str.contains(",").any():
        if "second_contact_phone" in df.columns:
            # split phone numbers by comma and add to second_contact_phone
            df["phone"], df["temp_phone"] = df["phone"].str.split(",", 1).str
            df["second_contact_phone"] = (
                    df["second_contact_phone"].astype(str).fillna("")
                    + ", "
                    + df["temp_phone"].astype(str).fillna("")
            )
            del df["temp_phone"]
        if "second_contact_phone" not in df.columns:
            df["second_contact_phone"] = ""
            df["phone"], df["temp_phone"] = df["phone"].str.split(",", 1).str
            df["second_contact_phone"] = (
                    df["second_contact_phone"].astype(str).fillna("")
                    + ", "
                    + df["temp_phone"].astype(str).fillna("")
            )
            del df["temp_phone"]

    # only keep numbers in phone column
    df["phone"] = df["phone"].replace(to_replace=r"[^0-9]+", value="", regex=True)

    # if there is a bad phone then do stuff and hopefully stop adding a second_contact_phone when its not needed
    if df.phone.astype(str).str.contains("^(?:(?!^.{,7}$|^.{16,}$).)*$").any():
        if "second_contact_phone" in df.columns:
            # moves phone numbers less than 8 and greater than 15 digits then removes them from phone
            df["temp_second_contact_phone"] = df[
                ~df["phone"]
                    .astype(str)
                    .str.contains(pat=r"^(?:(?!^.{,7}$|^.{16,}$).)*$", case=False, na=False)
            ]["phone"]
            df["phone"] = df[
                df["phone"].astype(str).str.contains(pat=r"^(?:(?!^.{,7}$|^.{16,}$).)*$", case=False, na=False)
            ]["phone"]
            # merges columns so original second_contact_email doesn't get replaced by temp_second_contact_email
            df["second_contact_phone"] = (
                    df["second_contact_phone"].astype(str).fillna("")
                    + ", "
                    + df["temp_second_contact_phone"].astype(str).fillna("")
            )
            del df["temp_second_contact_phone"]
            # this is to just clean up column (i.e. remove leadning what space,
            # random extra commas from merge, and random .0)
            df["second_contact_phone"] = df["second_contact_phone"].replace(
                to_replace=r"((, )$|[,]$|(^\s)|(\.0))", value="", regex=True
            )
            # definitely not needed but one case bothered me so I added it
            df["second_contact_phone"] = df["second_contact_phone"].replace(
                to_replace=r"(  )", value=" ", regex=True
            )
        else:
            if "second_contact_phone" not in df.columns:
                df["second_contact_phone"] = ""
                # moves phone numbers less than 8 and greater than 15 digits then removes them from phone
                df["temp_second_contact_phone"] = df[
                    ~df["phone"]
                        .astype(str)
                        .str.contains(
                        pat=r"^(?:(?!^.{,7}$|^.{16,}$).)*$", case=False, na=False
                    )
                ]["phone"]
                df["phone"] = df[
                    df["phone"]
                        .astype(str)
                        .str.contains(
                        pat=r"^(?:(?!^.{,7}$|^.{16,}$).)*$", case=False, na=False
                    )
                ]["phone"]
                # merges columns so original second_contact_email doesn't get replaced by temp_second_contact_email
                df["second_contact_phone"] = (
                        df["second_contact_phone"].astype(str).fillna("")
                        + ", "
                        + df["temp_second_contact_phone"].astype(str).fillna("")
                )
                del df["temp_second_contact_phone"]
                # this is to just clean up column (i.e. remove leadning what space, random extra
                # commas from merge, and random .0)
                df["second_contact_phone"] = df["second_contact_phone"].replace(
                    to_replace=r"((, )$|[,]$|(^\s)|(\.0))", value="", regex=True
                )
                # definitely not needed but one case bothered me so I added it
                df["second_contact_phone"] = df["second_contact_phone"].replace(
                    to_replace=r"(  )", value=" ", regex=True
                )

    return df


def merge_rows(df, merge_on):
    sssss = time.time()

    # filtering out columns 'first_name', 'last_name', 'email', 'contact_id' from df
    # so merger doesn't merge these columns, it just keeps the first instance of them
    new_df = df[df.columns.difference(["first_name", "last_name", "email"])]

    # marks the first instance of a duplicate as true
    df["first_dupe"] = df.duplicated(merge_on, keep=False) & ~df.duplicated(
        merge_on, keep="first"
    )

    # https://github.com/khalido/notebooks/blob/master/pandas-dealing-with-dupes.ipynb
    def combine_rows(row, key=merge_on, cols_to_combine=new_df):
        # takes in a row, looks at the key column if its the first dupe, combines the data in cols_
        # to_combine with the other rows with same key needs a dataframe with a bool column first_
        # dupe with True if the row is the first dupe
        if row["first_dupe"] is True:
            # making a df of dupes item
            dupes = df[df[key] == row[key]]

            # skipping the first row, since thats our first_dupe
            for i, dupe_row in dupes.iloc[1:].iterrows():
                for col in cols_to_combine:
                    dupe_row[col] = str(dupe_row[col])
                    row[col] = str(row[col])
                    # so fields don't have multiple of the same thing because of the merge
                    # e.g. buyer,buyer because 2 merged rows have the type buyer,
                    # now it just puts buyer there once
                    if row[col].lower() not in dupe_row[col].lower():
                        row[col] += ", " + dupe_row[col]
            # make sure first_dupe doesn't get processed again
            row.first_dupe = False
        return row

    df = df.apply(combine_rows, axis=1)
    # drops dup emails but keep first instance since everything should have been
    # merged into the first instance but ignores cells that are empty because before it would
    # just delete all rows with an empty email cell but the first one.....
    df = df[
        df[merge_on].isnull()
        | ~df[df[merge_on].notnull()].duplicated(subset=merge_on, keep="first")
        ]
    df.groupby(merge_on).agg(lambda x: ", ".join(x)).reset_index()
    del df["first_dupe"]

    fffff = time.time() - sssss
    print(f"Merger took {fffff} seconds.")
    return df


def cleanup(df):
    # gets rid of random nan that pops up sometimes
    df = df.replace(to_replace=r"(?:^|\W)nan(?:$|\W)", value="", regex=True)
    # these three just cleans up the file and gets rid of random commas.
    # Not really necessary and aren't perfect but you know makes the file less ugly
    df = df.replace(to_replace=r"^(, )|^(,)", value="", regex=True)
    df = df.replace(to_replace=r"(, , )", value=", ", regex=True)
    df = df.replace(
        to_replace=r"[,]{1}$", value="", regex=True
    )  # removes trailing commas

    return df


if __name__ == "__main__":
    start = time.time()
    main()
    finish = time.time() - start
    print(f"CSV has been printed in {finish} seconds.")
