import csv
import os

# Function to split source CSV into multiple files based on the 'type' column
def split_csv_by_type(source_file):
    # Get the directory of the current script
    output_dir = os.path.dirname(os.path.abspath(__file__))

    # Read the source file
    with open(source_file, mode='r', newline='', encoding='utf-8') as file:
        reader = csv.DictReader(file)
        
        # Collect rows by type
        type_groups = {}
        for row in reader:
            record_type = row['type']
            if record_type not in type_groups:
                type_groups[record_type] = []
            type_groups[record_type].append(row)

        # Write each type to its own CSV file
        for record_type, rows in type_groups.items():
            output_file = os.path.join(output_dir, f"{record_type}.csv")
            with open(output_file, mode='w', newline='', encoding='utf-8') as out_file:
                writer = csv.DictWriter(out_file, fieldnames=reader.fieldnames)
                writer.writeheader()
                writer.writerows(rows)

    print(f"Files successfully created in {output_dir}")

# Example usage
source_csv = "filestask_view.csv"  # Replace with the path to your source CSV file
split_csv_by_type(source_csv)