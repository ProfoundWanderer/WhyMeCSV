# WhyMeCSV

This script is meant to help clean up CSV files in hopes they are imported without being rejected and too match as many columns to fields in the Real Geeks CRM as possible.

## Installation

Download files and run them or just use http://profoundwanderer.pythonanywhere.com

The Github for the web version is [here](https://github.com/ProfoundWanderer/csv-formatter), however, it isn't quite up to date since I work out bugs here then move the code over.

## Things It Does

1. Deletes all empty rows and columns
2. Attempts to rename columns for quicker matching during the import process
3. Puts first name as first column followed by last name, and email, and phone column (This is order is needed for merging duplicate emails)
4. It moves any first name and last names that are longer than 256 characters to a new column (256 is the max character count for these fields in the CRM)
5. If the email column contains a comma then it spilts on the comma and puts what was split off into the second_contact_email column (It will create this column if it doesn't exist)
6. Moves any invalid emails to the the second_contact_email column (It will create this column if it doesn't exist)
7. Searches the rows for duplicate emails and if it finds any then it merges the rows
8. Moves any invalid phone numbers to the the second_contact_phone column (It will create this column if it doesn't exist)
9. Cleans up the file the about (e.g. removes extra commas, nan, spaces, etc.)

## Contributing
Pull requests are welcome or just let me know any issues or things that may make it better. Planning to combine it with leadalicious in the future.

Please make sure to update tests as appropriate.

## License
[MIT](https://choosealicense.com/licenses/mit/)