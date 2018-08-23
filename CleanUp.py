import pandas as pd


# point to file location.
filename = "/home/ProfoundWanderer/WhyMeCSV/stuff_in_sec_contact.csv"
df = pd.read_csv(filename)
df.columns = [i.lower().replace(' ', '_') for i in df.columns] #lower case and replace spaces
df.index += 2  # so when it says "check these lines" the numbers match with csv

# assuming email is 3rd field use row [2] - which it is for the RG template
email_list = df['email']  # <- edit: [1:] so that we ignore header
email_counts = email_list.value_counts()
duplicate_emails = list(email_counts[email_counts > 1].index)
print(f'Check these emails for duplicates: {duplicate_emails}')


# Next goal is to pull list of phone numbers from row 3 that have less than 8 digits
phone_list = df['phone']
phone_list = phone_list.replace('[^0-9]+', '', regex=True)  # remove special characters
phone_len = phone_list.str.len()
phone_bad = list(phone_len[(phone_len > 0) & (phone_len < 8)].index)
print(f'Check these lines for an incomplete phone number: {phone_bad}')


if (email_list.str.contains(',')).any():
    # splits email list so that everything before comma is put in email and the rest into secondary_email
    df[['email', 'secondary_email']] = email_list.str.split(',', 1).apply(pd.Series)


    df.columns = df.columns.fillna('secondary_email')

    if 'second_contact_email' in df.columns: # this handles in case they do have the 'second_contact_email' column
        # Merges secondary_email into the 'second_contact_email' column
        df['second_contact_email'] = df['second_contact_email'].astype(str)+ ',' +df['secondary_email']
        df = df.iloc[:, :-1]  # drops last column so there isn't double 'second_contact_email'

else:
    print('this was an else statement')


# Validate email: [^@]+@[^@]+\.[^@]+ (it has exactly one @ sign and at least one . after the @)
# (^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$) (another regex that can be used to verify emails)
df['email'] = df['email'].str.replace('^(.(?!([^@]+@[^@]+\.[^@]+)))*$', '')


# searches column named email and drops duplicates but keeps the first one
df['email'] = df['email'].drop_duplicates(keep='first')



# this adds 'second_contact_phone' in case they do not have the column
phone_value = ''
if 'second_contact_phone' not in df:
    df.loc[:,'second_contact_phone'] = phone_value


# Finds all numbers in phone that are less than 8 digits and moves them
less_than_8 = df['phone'].str.findall('^.{,7}$')
df['second_contact_phone'] = df['second_contact_phone'].astype(str) + ',' + less_than_8.astype(str)
# remove the random stuff but keep commas
df['second_contact_phone'] = df['second_contact_phone'].replace('[^0-9,,]', '', regex=True)


# only keep numbers in phone column. without this sometimes the remove dup section below doesn't work 100%
df['phone'] = df['phone'].replace('[^0-9]+', '', regex=True)
# Removes "invalid" phone numbers from phone column.
df['phone'] = df['phone'].replace('^.{,7}$', '', regex=True)


df.to_csv('/home/ProfoundWanderer/WhyMeCSV/Done.csv', index=False)
print('CSV has been printed.')


