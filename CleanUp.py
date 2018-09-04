import pandas as pd
import numpy as np

# point to file location.
filename = 'C:/Users/Free/Desktop/try_combine_rows.csv'
df = pd.read_csv(filename)
df.columns = [i.lower().replace(' ', '_') for i in df.columns]  # lower case and replace spaces
df.index += 2  # so when it says "check these lines" the numbers match with csv


email_list = df['email']
email_counts = email_list.value_counts()
duplicate_emails = list(email_counts[email_counts > 1].index)
print(f'Check these emails for duplicates: {duplicate_emails}')

df.rename(columns={'direct_no': 'phone'}, inplace=True)

# Next goal is to pull list of phone numbers from row 3 that have less than 8 digits
phone_list = df['phone']
phone_list = phone_list.replace('[^0-9]+', '', regex=True)  # remove special characters
phone_len = phone_list.str.len()
phone_bad = list(phone_len[(phone_len > 0) & (phone_len < 8)].index)
print(f'Check these lines for an incomplete phone number: {phone_bad}')

get_col_headers = str(list(df[2:]))
col_headers = eval(get_col_headers)

if (email_list.str.contains(',')).any():
    # splits email list so that everything before comma is put in email and the rest into secondary_email
    df[['email', 'secondary_email']] = email_list.str.split(',', 1).apply(pd.Series)

    df.columns = df.columns.fillna('secondary_email')

    if 'second_contact_email' in df.columns:  # this handles in case they do have the 'second_contact_email' column
        # Merges secondary_email into the 'second_contact_email' column
        df['second_contact_email'] = df['second_contact_email'].astype(str) + ',' + df['secondary_email']
        df = df.iloc[:, :-1]  # drops last column so there isn't double 'second_contact_email'

else:
    pass


# Validate email: [^@]+@[^@]+\.[^@]+ (it has exactly one @ sign and at least one . after the @)
# (^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$) (another regex that can be used to verify emails)
df['email'] = df['email'].str.replace('^(.(?!([^@]+@[^@]+\.[^@]+)))*$', '')


# this adds 'second_contact_phone' in case they do not have the column
phone_value = ''
if 'second_contact_phone' not in df:
    df.loc[:, 'second_contact_phone'] = phone_value


# Finds all numbers in phone that are less than 8 digits and moves them
invalid_number = df['phone'].str.findall('^.{,7}$|^.{16,}$')
df['second_contact_phone'] = df['second_contact_phone'].astype(str) + ',' + invalid_number.astype(str)
# remove the random stuff but keep commas
df['second_contact_phone'] = df['second_contact_phone'].replace('[^0-9,,]', '', regex=True)


# only keep numbers in phone column. without this sometimes the remove dup section below doesn't work 100%
df['phone'] = df['phone'].replace('[^0-9]+', '', regex=True)
# Removes "invalid" phone numbers from phone column. Less than 7 and greater than 15 but doesn't move them
df['phone'] = df['phone'].replace('^.{,7}$|^.{16,}$', '', regex=True)

# removes random extra commas. probably could combine with line 58
df['second_contact_phone'] = df['second_contact_phone'].replace('[,]', '', regex=True)

# searches column named email and drops duplicates but keeps the first one and merges data
# https://github.com/khalido/notebooks/blob/master/pandas-dealing-with-dupes.ipynb

df["first_dupe"] = df.duplicated("email", keep=False) & ~df.duplicated("email", keep="first")


def combine_rows(row, key="email", cols_to_combine=col_headers[3:]):
    """takes in a row, looks at the key column
        if its the first dupe, combines the data in cols_to_combine with the other rows with same key
        needs a dataframe with a bool column first_dupe with True if the row is the first dupe"""

    if row['first_dupe'] is True:
        # making a df of dupes item
        dupes = df[df[key] == row[key]]

        for i, dupe_row in dupes.iloc[1:].iterrows():   # skipping the first row, since thats our first_dupe
            for col in cols_to_combine:
                row[col] += ", " + dupe_row[col]
        # make sure first_dupe doesn't get processed again
        row.first_dupe = False
    return row


df = df.apply(combine_rows, axis=1, result_type=None)
df.drop_duplicates(subset=["email"], inplace=True)
df.groupby('email').agg(lambda x: ", ".join(x)).reset_index()


# this drops the "first_dupe" column from above fuction
del df['first_dupe']


df.to_csv('C:/Users/Free/Desktop/Done.csv', index=False)
print('CSV has been printed.')


