1. ./setup.sh  # This installs dbt
2. ./load_external_gcs.sh  # This creates the initial table definition where everything is a string
3. cd college-scorecard 
4. dbt run   # This creates a table and a view.
