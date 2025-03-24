def clean_material_data(data):
    import pandas as pd

    if not data:
        print("\n❌ No unprocessed data found.")
        return []

    df = pd.DataFrame(data)

    # ✅ Fix Column Naming Issues
    column_mapping = {
        "SerialNoProfile": "Serial_No_Profile",
        "ReplacementPart": "Replacement_Part",
        "UsedInSBom": "Used_In_SBom"
    }
    df.rename(columns=column_mapping, inplace=True)

    required_columns = ['Serial_No_Profile', 'Replacement_Part']

    missing_columns = [col for col in required_columns if col not in df.columns]
    if missing_columns:
        print(f"\n🚨 Missing columns: {missing_columns}. Skipping processing.")
        return []  # Return empty list if critical columns are missing

    serial_no_profiles = ['ZPP2', 'ZPP8', 'ZCS1']

    df['Violation'] = (
        (df['Serial_No_Profile'].isin(serial_no_profiles)) & 
        (df['Replacement_Part'] != 'B')
    ).astype(int)

    df.loc[df['Violation'] == 1, 'Replacement_Part'] = 'B'
    df['cleaned(Unprocessed)'] = True

    return df.to_dict('records')
