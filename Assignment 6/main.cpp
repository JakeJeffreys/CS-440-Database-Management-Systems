#include <iostream>
#include <fstream>
#include <sstream>
#include <vector>
#include <algorithm>
#include <string>
#include <cstdlib>
#include <ctime>
#include <stdlib.h>
using namespace std;


const bool CRASH_FLAG = true; // See line 183 for crash simulation


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

bool crash_status;
string lastStatus;

vector<deptTuple> bufferD;
vector<empTuple> bufferE;

vector<int> runLocations;

void parseAndWriteToBuffer(string inputTuple, string deptOrEmp="dept");
void mergeAndWrite(empTuple emp, deptTuple dept);

void pullEmpIntoBuffer(int index);
void pullDeptIntoBuffer(int index);

void joinOperation();

void log_data(int indexE, int indexD);
bool check_for_crash();


int main()
{
  srand ( time(NULL) );

  crash_status = check_for_crash();
  if(CRASH_FLAG == false || crash_status == false){
    if( remove( "LOG.csv" ) != 0 )
      cout << "No Log file to delete." << endl;
    else
      cout << "Log file successfully cleared." << endl;

    if( remove( "join.csv" ) != 0 )
      cout << "No Join file to delete." << endl;
    else
      cout << "Join file successfully cleared." << endl;
  }

/*****************************************************************************
 SORT DEPT TABLE
******************************************************************************/
  cout << "Sorting..." << endl;
  string inputTuple;
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
      }

    }

    // Sort contents of buffer
    sort(bufferD.begin(), bufferD.end(), depttuple);


    // Save buffer to memory
    ostringstream oss;
    oss << "deptSorted.csv";
    fileName = oss.str();

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
    csvFile.close();

  }
  else cout << "Unable to open file";


/*****************************************************************************
 PULL RUNS INTO BUFFER
******************************************************************************/

  string bufferOutput;
  string inputRunItem;

/*------------------------ Pull in Dept runs --------------------------------*/

  ostringstream oss;
  oss << "deptSorted.csv";
  fileName = oss.str();

  ifstream drun (fileName.c_str());

  // Read data into bufferD
  if (drun.is_open())
  {
    getline (drun,inputRunItem);
    parseAndWriteToBuffer(inputRunItem);
  }
  drun.close();


  // cout << "Printing BufferD.." << endl;
  // for (int i=0 ; i<bufferD.size() ; i++){
  //   cout << bufferD[i].did << ", " <<
  //           bufferD[i].dname << ", " <<
  //           bufferD[i].budget << ", " <<
  //           bufferD[i].managerid << endl;
  // }


  /*------------------------ Pull in Emp runs --------------------------------*/

  pullEmpIntoBuffer(0);

  // cout << "Printing BufferE.." << endl;
  // for (int i=0 ; i<bufferE.size() ; i++){
  //   cout << bufferE[i].eid << ", " <<
  //           bufferE[i].ename << ", " <<
  //           bufferE[i].age << ", " <<
  //           bufferE[i].salary << endl;
  // }

/*****************************************************************************
 MERGE DATA
******************************************************************************/
  cout << "Merging..." << endl;

  joinOperation();


  return 0;
}


// End of main
/*----------------------------------------------------------------------------*/
void joinOperation(){
  int x=0;
  int randError = (rand() % 8);
  int indexE, indexD;
  vector<string> log_info;


  if (CRASH_FLAG == false || crash_status == false){
    indexE=0, indexD=0;
  } else {
    stringstream ss( lastStatus );

    while( ss.good() )
    {
      string substr;
      getline( ss, substr, ',');
      log_info.push_back(substr);
      cout << "DATA: " << substr << endl;
    }

    indexE = stoi(log_info[0]);
    indexD = stoi(log_info[1]);
  }

  while(bufferE.size() > 0 && bufferD.size() > 0 ){

    // Recovery system
    log_data(indexE, indexD);


    if (bufferE[0].eid < bufferD[0].managerid){

      indexE++;
      pullEmpIntoBuffer(indexE);

    } else if (bufferE[0].eid == bufferD[0].managerid) {

      mergeAndWrite(bufferE[0], bufferD[0]);

      // CRASH SIMULATION
      if (CRASH_FLAG == true){
        if (randError == 10){
          cout << "CRASH TRIGGERED" << endl;
          abort();
        }
        randError++;
      }


      indexE++;
      indexD++;

      pullEmpIntoBuffer(indexE);
      pullDeptIntoBuffer(indexD);

    } else {

      indexD++;
      pullDeptIntoBuffer(indexD);

    }

    x++;
  }

  // Record successful join
  ofstream log_data;
  log_data.open ("LOG.csv", ios_base::app);
  log_data << "SUCCESS";
  log_data.close();

}



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
  bufferE.clear();
  string inputRunItem;
  ifstream erun ("Emp.csv");

  // Read data into bufferE
  if (erun.is_open())
  {
    for(int line=0; getline(erun,inputRunItem); line++){
      if( line == index && !inputRunItem.empty() ){ // Start at specfic line
        inputRunItem.erase(remove(inputRunItem.begin(), inputRunItem.end(), '\"'), inputRunItem.end());
        parseAndWriteToBuffer(inputRunItem , "emp");
      }
    }
    erun.close();
  }

}

void pullDeptIntoBuffer(int index) {
  bufferD.clear();
  string inputRunItem;
  ifstream drun ("deptSorted.csv");

  // Read data into bufferD
  if (drun.is_open())
  {
    for(int line=0; getline(drun,inputRunItem); line++){

      if (line == index && !inputRunItem.empty() ){ // Start at specfic line
        parseAndWriteToBuffer(inputRunItem);
      }
    }

    drun.close();
  }
}


void log_data(int indexE, int indexD){
  ofstream log_data;
  log_data.open ("LOG.csv", ios_base::app);
  log_data << indexE << ", " << indexD << "\n";
  log_data.close();
}

bool check_for_crash(){
  ifstream logFile ("LOG.csv");
  string log_line;

  if (logFile.is_open())
  {
    for(int line=0; getline(logFile, log_line); line++){
      lastStatus = log_line;
      if (lastStatus == "SUCCESS"){
        return false;
      }
    }
    logFile.close();
  }

  return true;

}
