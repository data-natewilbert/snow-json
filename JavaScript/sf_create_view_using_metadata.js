CREATE OR REPLACE PROCEDURE create_view_using_metadata (DATA_BASE varchar, SCHEMA_NAME varchar, V_TAB_NM varchar, COLUMN_CASE varchar, COLUMN_TYPE varchar)
RETURNS VARCHAR
LANGUAGE javascript
AS
$$

////////////////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////////////////
// 
// CREATE_VIEW_USING_METADATA - Nate Wilbert - July 2020
//
//
// Purpose: 
// Take a user input table that contains a variant datatype column (json) and then ...
// for non-variant datatype, recreate column and data type and include new view ...
// for variant datatype, apply the logic from Jan 15, 2020 Snowflake blog post by Craig Warman 
// which describes how to handle JSON data in a more automated manner, for a single column
// this script expands the ability to do it from a table with one column only, to a table with
// multiple columns - and it employs the logic Craig shared in his second blog post, with some
// tweaks. 
//
//
// Note: Some JSON fields may not have any data. They are not re-created in the view.
// Tip: When making modifications and troubleshooting the code, use the HISTORY tab in Snowflake
// ... HISTORY will allow you to see any statements created along the way, and any dynamic DDL
// ... and when your looking thru SQL history make sure to refresh (top right) the page.
//
//
// Parameters:
// DATA_BASE	 - Name of the database holding table with JSON, to query INFORMATION_SCHEMA metadata 
// SCHEMA_NAME	 - Name of the schema holding the table, for prefixing in the code
// TABLE_NAME    - Name of table that contains the semi-structured data.
// COLUMN_CASE   - Defines whether or not view column name case will match
//                 that of the corresponding JSON document attributes.  When
//                 set to 'uppercase cols' the view column name for a JSON 
//                 document attribute called "City.Coord.Lon" would be generated 
//                 as "CITY_COORD_LON", but if this parameter is set to 
//                 'match col case' then it would be generated as "City_Coord_Lon".
// COLUMN_TYPE   - The datatypes of columns generated for the view will match
//                 those of the corresponding JSON data attributes if this param-
//                 eter is set to 'match datatypes'. But when this parameter is 
//                 set to 'string datatypes' then the datatype of all columns 
//                 in the resulting view will be set to STRING (VARCHAR) or ARRAY.
// 				 - See Craig's blog posts for discussion around why these params can be helpful.
//
//
// Usage Example:
// call create_view_using_metadata ('EDW_DEV', 'SENSOR_SCHEMA','SENSOR_NOTIFICATION_ALL','uppercase cols','string datatypes');
// 
//
// Attribution:
// Craig Warman @ Snowflake
// 		https://www.snowflake.com/blog/automating-snowflakes-semi-structured-json-data-handling-part-2/
// 		https://www.snowflake.com/blog/automating-snowflakes-semi-structured-json-data-handling/
// Alan Eldridge @ Snowflake
// 		Alan wrote code which Craig took and worked to create the two blog entries
//
//
// Helpful Snowflake Documentation:
// 
//	https://docs.snowflake.com/en/sql-reference/info-schema/columns.html		// Metadata is key to this version of the code, get familiar with MD available
//	https://docs.snowflake.com/en/sql-reference/stored-procedures-api.html		// Using Javascript procedure API
//	https://docs.snowflake.com/en/sql-reference/functions/flatten.html			// Key function to break out the JSON data from it's VARIANT black box field
//	https://docs.snowflake.com/en/sql-reference/functions-regexp.html			// Regular Expression usd to clearn up JSON data as needed 
// 	https://docs.snowflake.com/en/sql-reference/functions/regexp_replace.html
//
////////////////////////////////////////////////////////////////////////////////////////////////

// VARIABLES AND INITIALIZATION 

var path_name = "regexp_replace(regexp_replace(f.path,'\\\\[(.+)\\\\]'),'(\\\\w+)','\"\\\\1\"')";    // This generates paths with levels enclosed by double quotes (ex: "path"."to"."element").  It also strips any bracket-enclosed array element references (like "[0]")
var attribute_type = "DECODE (substr(typeof(f.value),1,1),'A','ARRAY','B','BOOLEAN','I','FLOAT','D','FLOAT','STRING')";    // This generates column datatypes of ARRAY, BOOLEAN, FLOAT, and STRING only
var alias_name = "REGEXP_REPLACE(REGEXP_REPLACE(f.path, '\\\\[(.+)\\\\]'),'[^a-zA-Z0-9]','_')";    // This generates column aliases based on the path
var table_list = SCHEMA_NAME + "." + V_TAB_NM;
var col_list = ""; // This collects column data. For VARIANT columns: name, data type, alias. For OTHER columns: just column_name.
var alias_dbl_quote = "";
var array_num = 0;

////////////////////////////////////////////////////////////////////////////////////////////////

// PARAMETER RELATED LOGIC: CASE OF COLUMN HEADERS, DATA TYPE OF COLUMNS FROM JSON

if (COLUMN_CASE.toUpperCase().charAt(0) == 'M') {
   alias_dbl_quote = "\""; }          // COLUMN_CASE parameter is set to 'match col case' so add double quotes around view column alias name 
if (COLUMN_TYPE.toUpperCase().charAt(0) == 'S') {
   attribute_type = "DECODE (typeof(f.value),'ARRAY','ARRAY','STRING')"; }   // COLUMN_TYPE parameter is set to 'string datatypes' so typecast to STRING instead of value returned by TYPEPOF function


// GRAB TABLE METADATA: USING DATABASE, SCHEMA, AND TABLE NAME

var sqlMetaQryBld = "select distinct COLUMN_NAME \n" + 
					", DATA_TYPE \n" +
					"from " + DATA_BASE + "." + "information_schema.columns \n" +
					"where table_name = '" + V_TAB_NM + "'\n" +
					"and table_schema = '" + SCHEMA_NAME + "'"; 
					// Future Enhancement: Would be nice to order the column results


var sqlMetaQryStmt = snowflake.createStatement({sqlText:sqlMetaQryBld}); 
var sqlMetaRes = sqlMetaQryStmt.execute();

////////////////////////////////////////////////////////////////////////////////////////////////

// LOOP THROUGH EACH FIELD OF THE TABLE, DISTINGUISHING BETWEEN VARIANT AND NON-VARIANT 

while (sqlMetaRes.next()) {

// For each element returned in the results: if VARIANT apply logic A else apply logic B

	if (sqlMetaRes.getColumnValue(2) == 'VARIANT') {

		// Where we flatten a JSON data element into additional columns for the final view
		var element_query = "SELECT DISTINCT \n" +
			path_name + " AS path_name, \n" +
			attribute_type + " AS attribute_type, \n" +
			alias_name + " AS alias_name \n" +
			"FROM " + table_list + ", \n" +
			"LATERAL FLATTEN(" + sqlMetaRes.getColumnValue(1) + ", RECURSIVE=>true) f \n" +
			"WHERE TYPEOF(f.value) != 'OBJECT' \n" +
			"AND NOT contains(f.path,'[') ";      // This prevents traversal down into arrays; 
		
		// Run the query...
		var element_stmt = snowflake.createStatement({sqlText:element_query});
		var element_res = element_stmt.execute();

		// ...And loop through the list that was returned
		// Added restriction that path must be populated, 
		// to prevent null only values from showing and causing mischief
		while (element_res.next() && element_res.getColumnValue(1) != "") {

			// Add elements and datatypes to the column list
			// They will look something like this when added: 
			//    col_name:"name"."first"::STRING as name_first, 
			//    col_name:"name"."last"::STRING as name_last   

			// Add any non-array elements and datatypes to the column list
			// They will look something like this when added: 
			//    col_name:"name"."first"::STRING as "name_first", 
			//    col_name:"name"."last"::STRING as "name_last"
			// Note that double-quotes around the column aliases will be added
			// only when the COLUMN_CASE parameter is set to 'match col case'   

			if (element_res.getColumnValue(2) != 'ARRAY') {


				if (col_list != "") {
					col_list += ", \n";}
				col_list += sqlMetaRes.getColumnValue(1) + ":" + element_res.getColumnValue(1);   					// Start with the element path name
				col_list += "::" + element_res.getColumnValue(2);             										// Add the datatype
				col_list += " as " + sqlMetaRes.getColumnValue(1) + "_" + element_res.getColumnValue(3);           	// And finally the element alias 
			
			}

			// Array elements get handled in the following section:
		   else {
			  array_num++;
			  var simple_array_col_list = "";
			  var object_array_col_list = "";

			// Build a query that returns the elements in the current array
			  var array_query = "SELECT DISTINCT \n"+
								 path_name + " AS path_name, \n" +
								 attribute_type + " AS attribute_type, \n" +
								 alias_name + " AS attribute_name, \n" +
								 "f.index \n" +
								 "FROM " + table_list + ", \n" +
								 "LATERAL FLATTEN(" + sqlMetaRes.getColumnValue(1) + ":" + element_res.getColumnValue(1) + ", RECURSIVE=>true) f \n" +
								 "WHERE REGEXP_REPLACE(f.path, '.+(\\\\w+\\\\[.+\\\\]).+', 'SubArrayEle') != 'SubArrayEle' ";  // This prevents return of elements of nested arrays (the entire array will be returned in this case)

			// Run the query...
			  var array_stmt = snowflake.createStatement({sqlText:array_query});
			  var array_res = array_stmt.execute();

			// ...And loop through the list that was returned.
			// Add array elements and datatypes to the column list
			// The way that they're added depends on the type of array:
			//
			// Simple arrays: 
			// These are lists of values that are addressible by their index number
			//   For example: 
			//      "code": {
			//         "rgb": [255,255,0]
			// These will be added to the view column list like so:
			//    col_name:"code"."rgb"[0]::FLOAT as code_rgb_0, 
			//    col_name:"code"."rgb"[1]::FLOAT as code_rgb_1, 
			//    col_name:"code"."rgb"[2]::FLOAT as code_rgb_2
			//
			// Object arrays:
			// Collections of objects that addressible by key
			// For example:
			//     contact: {
			//         phone: [
			//           { type: "work", number:"404-555-1234" },
			//           { type: "mobile", number:"770-555-1234" } 
			// These will be added to the view column list like so:
			//    a1.value:"type"::STRING as "phone_type",
			//    a1.value:"number"::STRING as "phone_number"
			// Along with an additional LATERAL FLATTEN construct in the table list:
			//    FROM mydatabase.public.contacts,
			//     LATERAL FLATTEN(json_data:"contact"."phone") a1;
			//

			  while (array_res.next()) {
				 if (array_res.getColumnValue(1).substring(1) == "") {              // The element path name is empty, so this is a simple array element
					 if (simple_array_col_list != "") {
						simple_array_col_list += ", \n";}
					 simple_array_col_list += sqlMetaRes.getColumnValue(1) + ":" + element_res.getColumnValue(1);   // Start with the element path name
					 simple_array_col_list += "[" + array_res.getColumnValue(4) + "]";           					// Add the array index
					 simple_array_col_list += "::" + array_res.getColumnValue(2);                					// Add the datatype
					 simple_array_col_list += " as " + alias_dbl_quote + sqlMetaRes.getColumnValue(1) + "_" + element_res.getColumnValue(3) + "_" + array_res.getColumnValue(4) + alias_dbl_quote;   // And finally the element alias - Note that the array alias is added as a prefix to ensure uniqueness
					 }
				 else {                                                             // This is an object array element
					 if (object_array_col_list != "") {
						object_array_col_list += ", \n";}
					 object_array_col_list += "a" + array_num + ".value:" + array_res.getColumnValue(1).substring(1);    // Start with the element name (minus the leading '.' character)
					 object_array_col_list += "::" + array_res.getColumnValue(2);                                        // Add the datatype
					 object_array_col_list += " as " + alias_dbl_quote + sqlMetaRes.getColumnValue(1) + "_" + element_res.getColumnValue(3) + array_res.getColumnValue(3) + alias_dbl_quote;   // And finally the element alias - Note that the array alias is added as a prefix to ensure uniqueness
					 }
			  }

		// If no object array elements were found then add the simple array elements to the 
		// column list...
			  if (object_array_col_list == "") {
				  if (col_list != "") {
					 col_list += ", \n";}
				  col_list += simple_array_col_list;
				  }
		// ...otherwise, add the object array elements to the column list along with a
		// LATERAL FLATTEN clause that references the current array to the table list
			  else {
				  if (col_list != "") {
					 col_list += ", \n";}
				  col_list += object_array_col_list;
				  table_list += ",\n LATERAL FLATTEN(" + sqlMetaRes.getColumnValue(1) + ":" + element_res.getColumnValue(1) + ") a" + array_num;
				  }
		   }
			
		}
	}
	else {

		if (col_list != "") {
			col_list += ", \n";
		}
		
		col_list += sqlMetaRes.getColumnValue(1); // Column name only needed, no datatype declaration
		}
}	

// Now build the CREATE VIEW statement
var view_ddl = "CREATE OR REPLACE VIEW " + V_TAB_NM + "_VW AS \n" +
	   "SELECT \n" + col_list + "\n" +
	   "FROM " + table_list;

// Now run the CREATE VIEW statement
var view_stmt = snowflake.createStatement({sqlText:view_ddl});
var view_res = view_stmt.execute();
return view_res.next();

$$