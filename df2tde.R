# df2tde.R

## A function takes a dataframe and creates a Python script to create a TDE. It then runs that script.
## pass in dataset and desired TDE name

df2tde <- function(dataf, tdename)
{
# get names and class of each column
colna <- colnames(dataf)
colnu <- ncol(dataf)
colcl <- c()

for (i in 1:colnu ){
  colcl <- c(colcl, toString(class(dataf[1,i]))) # convert class of colum to string and add to vector
}

# Clean up files from previous run
csvname <- paste0(tdename,".csv")
if (file.exists(csvname)) file.remove(csvname)

pyname <- paste0(tdename,".py")
if (file.exists(pyname)) file.remove(pyname)

fn <- paste0(tdename,".tde")
if (file.exists(fn)) file.remove(fn);

if (file.exists("DataExtract.log")) file.remove("DataExtract.log");


# write out CSV
# csvname <- paste0(tdename,".csv")
write.csv(dataf, file = csvname, quote=TRUE, na = "", row.names = FALSE)

# Open output file and write header
# pyname <- paste0(tdename,".py")
convertpy <- file(pyname, "w")
header <- paste0("# ",pyname,"
import csv,os,datetime
import dataextract as tde

#Step 1: Create the Extract File and open the .csv
tdefile = tde.Extract('",tdename,".tde')

csvReader = csv.reader(open('",csvname,"','rb'), delimiter=',')

#Step 2: Create the tableDef
tableDef = tde.TableDefinition()
", collapse = " ") 

cat(header, file = convertpy)
# tabledef
for (i in 1:colnu ){
  type <- switch(colcl[i],
         integer = "INTEGER",
         numeric = "DOUBLE",
         character = "CHAR_STRING",
         logical = "BOOLEAN",
         Date = "DATE"
         )
  line <- paste0("tableDef.addColumn('",colna[i],"', tde.Type.",type,")\n" )
  cat(line, file = convertpy)
}

code <- paste0("
#Step 3: Create the table in the image of the tableDef
table = tdefile.addTable('Extract',tableDef)

#Step 4: Loop through the csv, grab all the data, put it into rows
#and insert the rows into the table
newrow = tde.Row(tableDef)
csvReader.next() #Skip the first line since it has the headers
for line in csvReader:
")
cat(code, file = convertpy)

# code to insert rows
for (i in 1:colnu ){
  type <- switch(colcl[i],
                 integer = paste0("      newrow.setInteger(",i-1,",int(line[",i-1,"]))"),
                 numeric = paste0("      newrow.setDouble(",i-1,",float(line[",i-1,"]))"),
                 character = paste0("      newrow.setCharString(",i-1,", str(line[",i-1,"]))"),
                 logical = paste0("      newrow.setBoolean(",i-1,",bool(line[",i-1,"]))"),
                 Date = paste0("      date = datetime.datetime.strptime(line[",i-1,"], '%Y-%m-%d')
      newrow.setDate(",i-1,", date.year, date.month, date.day)")
  )
  line <- paste0("  try:
",type,"\n","  except ValueError:
      newrow.setNull(",i-1,")\n" )
  cat(line, file = convertpy)
}

code <- paste0("  table.insert(newrow)

#Step 5: Close the tde
tdefile.close()
\n")
cat(code, file = convertpy)
close(convertpy)
# Run The Python
cmd <- paste0("python ",pyname,"\n")
system(cmd, wait = FALSE)
}

