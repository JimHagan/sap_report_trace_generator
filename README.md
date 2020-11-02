# sap_report_trace_generator

This repository enables the creation of New Relic traces from ST03 metrics data that has been forwarded to New Relic by the Inforwarder agent.

## Getting Started

- Clone the repository
`git clone https://github.com/JimHagan/sap_report_trace_generator.git`

- Make sure you have the following environment variables created for the account you are working with:
    ```
    NR_TRACEGEN_INSIGHTS_QUERY_KEY
    NR_TRACEGEN_INSIGHTS_INSERT_KEY
    ```
- CD into the code rolderand build  a python3 virtualenv.  There are several ways to do this but this example assumes you have `virtualenv` installed...

    ```
    cd sap_report_trace_generator
    virtualenv ./venv
    pip install -r requirements.txt
    ```

- Run the code 

    ```
    python st03.py
    ```

- If there were no errors and there was some data to process you will see this message...


```
Number of recs with GUI component: [number recs]"
```


- Finally the program wil have written a file named traces.json which contains the same JSON data that was posted to the trace API
