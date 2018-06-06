#include <iostream>
#include <fstream>
#include <sstream>
#include <vector>
#include <algorithm>
#include <string>
using namespace std;


struct deptTuple {
  string did;
  string dname;
  string budget;
  int managerid;

  bool operator() (deptTuple i,deptTuple j) { return (i.managerid<j.managerid);}
} depttuple;

struct empTuple {
  int eid;
  string ename;
  string age;
  string salary;
};

vector<deptTuple> bufferD;
vector<empTuple> bufferE;

vector<int> runLocations;

void pullEmpIntoBuffer(int index);
void parseAndWriteToBuffer(string inputTuple, string deptOrEmp="dept");
void mergeAndWrite(empTuple emp, deptTuple dept);
void fillDeptBufferRun(int runIndex, int fileCount);


int main()
{

/*****************************************************************************
 SORT DEPT TABLE
******************************************************************************/
  cout << "Sorting..." << endl;
  string inputTuple;
  int fileIndex = 1;
  string fileName;

  ifstream csvFile ("Dept.csv");

  // Read data into buffer
  if (csvFile.is_open())
  {
    while ( getline (csvFile,inputTuple) )
    {
      inputTuple.erase(remove(inputTuple.begin(), inputTuple.end(), '\"'), inputTuple.end());

      if (inputTuple != ""){

        parseAndWriteToBuffer(inputTuple);

        // Break when buffer is maxed out
        if (bufferD.size() == 22){
          // Sort contents of buffer
          sort(bufferD.begin(), bufferD.end(), depttuple);

          // Save buffer to memory
          ostringstream oss;
          oss << "deptRun" << fileIndex << ".csv";
          fileName = oss.str();
          fileIndex++;

          ofstream runData;
          runData.open(fileName.c_str());
          for(int i=0; i < bufferD.size(); i++){
            runData << bufferD[i].did << "," <<
                        bufferD[i].dname << "," <<
                        bufferD[i].budget << "," <<
                        bufferD[i].managerid;
            runData << "\n";
          }
          runData.close();

          // Clear buffer
          bufferD.clear();

        }
      }
    }
    csvFile.close();

  }
  else cout << "Unable to open file";

  // Handle leftover buffer contents
  if (bufferD.size() > 0) {

    // Sort contents of buffer
    sort(bufferD.begin(), bufferD.end(), depttuple);

    // Save buffer to memory
    ofstream runData;
    runData.open ("deptRun0.csv", ios_base::app);
    for(int i=0; i < bufferD.size(); i++){
      runData << bufferD[i].did << "," <<
                  bufferD[i].dname << "," <<
                  bufferD[i].budget << "," <<
                  bufferD[i].managerid;
      runData << "\n";
    }
    runData.close();

    // Clear buffer
    bufferD.clear();

  }

/*****************************************************************************
 PULL RUNS INTO BUFFER
******************************************************************************/

  string bufferOutput;
  string inputRunItem;

/*------------------------ Pull in Dept runs --------------------------------*/

  // Initalize dept run locations
  for (int i = 0; i < fileIndex; i++) {
    runLocations.push_back(1);
  }

  int bufSize = 10/fileIndex;

  for (int i=0 ; i<fileIndex ; i++){

    ostringstream oss;
    oss << "deptRun" << i << ".csv";
    fileName = oss.str();

    ifstream drun (fileName.c_str());

    // Read data into bufferD
    if (drun.is_open())
    {
      for (int lineno = 0; getline (drun,inputRunItem) && bufferD.size() < (bufSize*(i+1)); lineno++){
        parseAndWriteToBuffer(inputRunItem);
      }
      drun.close();
    }

  }

  // for (int i=0 ; i<bufferD.size() ; i++){
  //   cout << bufferD[i].did << ", " <<
  //           bufferD[i].dname << ", " <<
  //           bufferD[i].budget << ", " <<
  //           bufferD[i].managerid << endl;
  // }


  /*------------------------ Pull in Emp runs --------------------------------*/

  pullEmpIntoBuffer(0);

  // for (int i=0 ; i<bufferE.size() ; i++){
  //   cout << bufferE[i].eid << ", " <<
  //           bufferE[i].ename << ", " <<
  //           bufferE[i].age << ", " <<
  //           bufferE[i].salary << endl;
  // }

/*****************************************************************************
 MERGE DATA
******************************************************************************/
  cout << "Merging..." << endl;;

  int dIndex = 0;
  int eIndex = 0;
  int empRunNumber = 1;

  for (int i=0 ; bufferE.size()>0 && !bufferE.empty() ; i++) {

    int runNumber = dIndex/bufSize;

    // cout << "(" << eIndex << " - " << dIndex << ") (Run #: " << runNumber << ") (";

    if (bufferE[eIndex].eid < bufferD[dIndex].managerid){

      // cout << "LESS) (" << bufferE[eIndex].eid << " - " << bufferD[dIndex].managerid << ")" << endl;

      // Switch to next dept run
      if (runNumber == (fileIndex-1)){
        if (eIndex == bufferE.size()-1){
          bufferE.clear();
          pullEmpIntoBuffer(empRunNumber);
          empRunNumber++;
          eIndex = 0;
        } else {
          eIndex++; // Increment emp
        }
        dIndex = 0; // Return to dept0
      } else {
        dIndex = (runNumber+1)*bufSize;
      }

    } else if (bufferE[eIndex].eid == bufferD[dIndex].managerid) {

      // cout << "EQUAL) (" << bufferE[eIndex].eid << " - " << bufferD[dIndex].managerid << ")" << endl;

      mergeAndWrite(bufferE[eIndex], bufferD[dIndex]);
      bufferD[dIndex].managerid = -1;

      if (dIndex == (runNumber+1)*bufSize-1) {
        // Refill dept buffer run
        fillDeptBufferRun(runNumber, fileIndex);
        dIndex = 0;

      } else {
        dIndex++;
      }

    } else {

      // cout << "MORE) (" << bufferE[eIndex].eid << " - " << bufferD[dIndex].managerid << ")" << endl;

      // Switch Runs
      if (dIndex == (runNumber+1)*bufSize-1) {
        // Refill dept buffer run
        fillDeptBufferRun(runNumber, fileIndex);
        dIndex = 0;
      } else {
        dIndex++;
      }

    }
  }


  return 0;
}


// End of main
/*----------------------------------------------------------------------------*/
void mergeAndWrite(empTuple emp, deptTuple dept){
  ofstream mergedData;
  mergedData.open ("join.csv", ios_base::app);
  mergedData << "\"" << dept.did << "\",\"" <<
                dept.dname << "\",\""  <<
                dept.budget << "\",\""  <<
                dept.managerid << "\",\"" <<
                emp.eid << "\",\"" <<
                emp.ename << "\",\"" <<
                emp.age << "\",\"" <<
                emp.salary << "\"";
  mergedData << "\n";
  mergedData.close();

  cout << dept.did << ", " <<
          dept.dname << ", "  <<
          dept.budget << ", "  <<
          dept.managerid << ", " <<
          emp.eid << ", " <<
          emp.ename << ", " <<
          emp.age << ", " <<
          emp.salary << endl;
}

void parseAndWriteToBuffer(string inputTuple, string deptOrEmp){

  vector<string> tuple;

  // Parse Tuple
  istringstream ss(inputTuple);
  string line;

  while(getline(ss, line))
  {
    size_t prev = 0, pos;
    while ((pos = line.find_first_of(",", prev)) != string::npos)
      {
        if (pos > prev)
          tuple.push_back(line.substr(prev, pos-prev));
        prev = pos+1;
      }
      if (prev < line.length())
        tuple.push_back(line.substr(prev, string::npos));
  }

  // Store tuple
  if (deptOrEmp == "dept") {
    deptTuple tempTuple;

    tempTuple.did = tuple[0];
    tempTuple.dname = tuple[1];
    tempTuple.budget = tuple[2];
    stringstream Managerid(tuple[3]);
    Managerid >> tempTuple.managerid;

    bufferD.push_back(tempTuple);

  } else {
    empTuple tempTuple;

    stringstream Eid(tuple[0]);
    Eid >> tempTuple.eid;
    tempTuple.ename = tuple[1];
    tempTuple.age = tuple[2];
    tempTuple.salary = tuple[3];

    bufferE.push_back(tempTuple);

  }
}


void pullEmpIntoBuffer(int index){
  string inputRunItem;
  ifstream erun ("Emp.csv");

  // Read data into bufferE
  if (erun.is_open())
  {
    for (int lineno = 0; getline (erun,inputRunItem) && bufferE.size() < 11; lineno++){
      if( lineno > index*11 && !inputRunItem.empty() ){ // Start at specfic line
        inputRunItem.erase(remove(inputRunItem.begin(), inputRunItem.end(), '\"'), inputRunItem.end());
        parseAndWriteToBuffer(inputRunItem , "emp");
      }
    }
    erun.close();
  }

}


void fillDeptBufferRun(int runIndex, int fileCount) {
  string inputRunItem;
  string fileName;
  string inputTuple;


  int bufSize = 10/fileCount;

  ostringstream oss;
  oss << "deptRun" << runIndex << ".csv";
  fileName = oss.str();

  ifstream drun (fileName.c_str());

  // Read data into bufferD
  if (drun.is_open())
  {
    int slot=0;
    bool emptyRun = true;

    for (int lineno = 0; getline (drun,inputRunItem); lineno++){

      if (lineno >= runLocations[runIndex]*bufSize && lineno < (runLocations[runIndex]+1)*bufSize){
        emptyRun = false;
        vector<string> tuple;

        // Parse Tuple
        istringstream ss(inputRunItem);
        string line;

        while(getline(ss, line))
        {
          size_t prev = 0, pos;
          while ((pos = line.find_first_of(",", prev)) != string::npos)
            {
              if (pos > prev)
                tuple.push_back(line.substr(prev, pos-prev));
              prev = pos+1;
            }
            if (prev < line.length())
              tuple.push_back(line.substr(prev, string::npos));
        }

        deptTuple tempTuple;

        tempTuple.did = tuple[0];
        tempTuple.dname = tuple[1];
        tempTuple.budget = tuple[2];
        stringstream Managerid(tuple[3]);
        Managerid >> tempTuple.managerid;

        bufferD[(runIndex*bufSize)+slot] = tempTuple;
        slot++;

      }

    }

    if (emptyRun == true){

      deptTuple tempTuple;

      tempTuple.did = "";
      tempTuple.dname = "";
      tempTuple.budget = "";
      tempTuple.managerid = 999999999;

      bufferD[(runIndex*bufSize)+slot] = tempTuple;
      slot++;
    }

    drun.close();
    runLocations[runIndex]++;

  }


}
