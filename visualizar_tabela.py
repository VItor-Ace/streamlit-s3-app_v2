import streamlit as st
import pandas as pd
import os
from datetime import datetime
import boto3
from io import BytesIO


s3 = boto3.client(
    's3',
    aws_access_key_id=st.secrets["AWS_ACCESS_KEY_ID"],
    aws_secret_access_key=st.secrets["AWS_SECRET_ACCESS_KEY"],
    region_name=st.secrets["AWS_REGION"]
)

# Title
st.title("Parquet Client Data Editor")

bucket = st.secrets["BUCKET_NAME"]
key = st.secrets["PARQUET_KEY"]


@st.cache_data
def read_from_s3(bucket, key):
    """Read parquet file directly from S3"""
    response = s3.get_object(Bucket=bucket, Key=key)
    return pd.read_parquet(BytesIO(response['Body'].read()))


def write_to_s3(df, bucket, key):
    """Write dataframe to S3 as parquet"""
    buffer = BytesIO()
    df.to_parquet(buffer)
    buffer.seek(0)
    s3.put_object(Bucket=bucket, Key=key, Body=buffer)
    return True


# Sidebar: file selection or upload
st.sidebar.header("Load Data")
mode = st.sidebar.radio("Choose load mode:", ("Use S3 file", "Upload local file"))

# Load data based on selection
try:
    if mode == "Use S3 file":
        df = read_from_s3(bucket, key)
        st.sidebar.success(f'Loaded from S3: s3://{bucket}/{key}')
    else:
        uploaded = st.sidebar.file_uploader("Upload a Parquet file", type=["parquet"])
        if uploaded is not None:
            df = pd.read_parquet(uploaded)
            st.sidebar.success("File uploaded and loaded")
        else:
            st.sidebar.info("Please upload a file or use S3 file")
            st.stop()
except Exception as e:
    st.error(f"Error loading data: {str(e)}")
    st.stop()


# Main editor function
def main_editor(df: pd.DataFrame) -> pd.DataFrame:
    st.subheader("Edit Table")
    try:
        # Try newer data_editor first
        edited_df = st.data_editor(
            df,
            use_container_width=True,
            num_rows="dynamic",
            key="data_editor"
        )
    except AttributeError:
        # Fallback to experimental editor
        edited_df = st.experimental_data_editor(
            df,
            use_container_width=True,
            num_rows="dynamic",
            key="data_editor"
        )

    # Row deletion confirmation
    if len(edited_df) < len(df):
        code = st.text_input("Enter code '125' to confirm deletion", key="confirm_code")
        if st.button("Confirm Deletion"):
            if code == "125":
                st.success(f"Deleted {len(df) - len(edited_df)} row(s)")
            else:
                st.error("Incorrect code. No rows were deleted.")
                edited_df = df.copy()  # Revert changes

    return edited_df


# Display and edit data
edited_df = main_editor(df)

# Save functionality
st.subheader("Save Changes")
save_option = st.radio("Save to:", ("S3", "Local"))

if save_option == "S3":
    if st.button("Save to S3"):
        try:
            # Create backup first
            backup_key = f"backups/Controle_de_Processos_{datetime.now().strftime('%Y%m%d')}.parquet"
            s3.copy_object(
                Bucket=BUCKET_NAME,
                CopySource={'Bucket': bucket, 'Key': key},
                Key=backup_key
            )

            # Save edited version
            write_to_s3(edited_df, bucket, key)
            st.success(f'Saved to S3: s3://{bucket}/{key}')
            st.info(f'Backup created at s3://{bucket}/{key}')
        except Exception as e:
            st.error(f"Error saving to S3: {str(e)}")
else:
    save_path = st.text_input("Local save path:", value="Controle_de_Processos_edited.parquet")
    if st.button("Save locally"):
        try:
            edited_df.to_parquet(save_path)
            st.success(f"Saved locally as {save_path}")
        except Exception as e:
            st.error(f"Error saving locally: {str(e)}")

# Optional preview
if st.checkbox("Show DataFrame Summary"):
    st.write(edited_df.describe(include='all'))

# Footer
st.markdown("---")
st.markdown("""
**Application Notes:**
- Default loads from S3: `s3://controle-de-processos/Controle_de_Processos.parquet`
- Upload alternative files when needed
- All S3 operations use credentials from `~/.aws/credentials`
""")
