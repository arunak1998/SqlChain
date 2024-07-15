import streamlit as st
from app import getdb
import ast 
import pandas as pd
def main():

    st.set_page_config(page_title='Chat with Mysql',page_icon=":speech_ballon")
    selected_table=''
    st.title('Sql query with Langchain')
    with st.sidebar:
        st.subheader("Settings")
        st.write("This is Simple Q and A Bot Connect and Start")


        selected_table = st.selectbox('Select a Display process', ['Tabel','Visualization'])

       
   

    question=st.text_input("Question:")
    submit=st.button("Submit")
    print(selected_table)
    print(question)
    if submit:
        st.write("Submitted!")
        # Perform actions if question is not empty and selected_table is 'Tabel'
        if question and selected_table == 'Tabel':
           
            # Replace with your database query function
            # response = getdb(question)
            result,columns = getdb(question)
           
           
           
            st.header("Answer")
            try:
             result_data = ast.literal_eval(result)
             print(type(result_data))
             print(result_data)
           
             df = pd.DataFrame(result_data, columns=columns)
          
           
             st.table(df)  # Display DataFrame in Streamlit
            except ValueError as e:
                st.error(f"Error creating DataFrame: {e}")
        elif not question:
            st.warning("Please enter a question.")
        elif not selected_table :
            st.warning("Please select 'Tabel' from the dropdown.")








if __name__=='__main__':
    main()