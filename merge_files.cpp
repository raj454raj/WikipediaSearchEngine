#include<bits/stdc++.h>
#define ll long long int
using namespace std;
string indexDir, sortedIndexDir, completeFile, lastToken, secondaryIndexDir;
int numberOfFiles, linesWritten, sortedFileIndex, numOfLastTokens;
string secondaryIndex;
ofstream secIndFile;

// ----------------------------------------------------------------------------
string toString(int v) {
    /* C++ integer to string */
    ostringstream ss;
    ss << v;
    return ss.str();
}

// ----------------------------------------------------------------------------
int toInt(string s) {
    /* C++ string to integer */
    int val;
    istringstream ss(s);
    ss >> val;
    return val;
}

// ----------------------------------------------------------------------------
vector<string> &split(const string &s, char delim, vector<string> &elems) {
    /* Split a string by delimiter and return vector<string> */
    stringstream ss(s);
    string item;
    while (getline(ss, item, delim))
        elems.push_back(item);
    return elems;
}

// ----------------------------------------------------------------------------
bool heapCompare(const pair<vector<string>, int>& a, const pair<vector<string>, int>&b) {
    return (a.first[0] > b.first[0]);
}

// ----------------------------------------------------------------------------
// Main heap for merging sorted files
priority_queue<pair<vector<string>, int>,
               vector< pair<vector<string>, int> >,
               decltype(&heapCompare)> mainHeap(&heapCompare);

// ----------------------------------------------------------------------------
unsigned int hexToInt(string s) {
    unsigned int x;
    stringstream ss;
    ss << hex << s;
    ss >> x;
    return x;
}

// ----------------------------------------------------------------------------
bool postingsCompare(const string& a, const string& b) {
    vector<string> A, B;
    A = split(a, '-', A);
    B = split(b, '-', B);
    return toInt(A[1]) > toInt(B[1]);
}

// ----------------------------------------------------------------------------
string join(const vector<string>& vec, const char* delim) {
    stringstream res;
    copy(vec.begin(), vec.end(), ostream_iterator<string>(res, delim));
    return res.str();
}

// ----------------------------------------------------------------------------
void writeToFile(string dirname) {
    string filename = sortedIndexDir + dirname +
                      "file" + toString(sortedFileIndex);
    ofstream file(filename);
    file << completeFile;
    file.close();
}

// ----------------------------------------------------------------------------
void writeToSecondaryIndex() {
    secIndFile << secondaryIndex;
}

// ----------------------------------------------------------------------------
void merge(string dirname) {

    ifstream files[numberOfFiles];
    string filename, data;
    vector<string> tmpToken, postingsList;
    for(int i = 0 ; i < numberOfFiles ; ++i) {
        filename = indexDir + dirname + "file" + toString(i + 1);
        files[i].open(filename.c_str());
        files[i] >> data;
        if(data == "")
            return;
        tmpToken = split(data, '#', tmpToken);
        mainHeap.push(make_pair(tmpToken, i));
        tmpToken.clear();
    }
    pair< vector<string>, int > topElement, nextTop;
    string postingsString;
    while(!mainHeap.empty()) {

        topElement = mainHeap.top();
        postingsString = topElement.first[1] + ";";
        mainHeap.pop();

        if(files[topElement.second] >> data) {
            tmpToken.clear();
            tmpToken = split(data, '#', tmpToken);
            mainHeap.push(make_pair(tmpToken, topElement.second));
            tmpToken.clear();
        }

        if(!mainHeap.empty()) {
            // Merge the postings list
            nextTop = mainHeap.top();
            while(nextTop.first[0] == topElement.first[0]) {
                postingsString += nextTop.first[1] + ";";
                mainHeap.pop();

                if(files[nextTop.second] >> data) {
                    tmpToken.clear();
                    tmpToken = split(data, '#', tmpToken);
                    mainHeap.push(make_pair(tmpToken, nextTop.second));
                    tmpToken.clear();
                }

                if(mainHeap.empty())
                    break;
                nextTop = mainHeap.top();
            }
        }

        postingsList = split(postingsString, ';', postingsList);
        sort(postingsList.begin(), postingsList.end(), postingsCompare);
        linesWritten++;
        completeFile += topElement.first[0] + "#" + join(postingsList, ";") + "\n";
        lastToken = topElement.first[0];

        if(linesWritten == 2500) {
            numOfLastTokens++;
            secondaryIndex += lastToken + "\n";
            if(numOfLastTokens == 1000) {
                writeToSecondaryIndex();
                secondaryIndex = "";
            }
            writeToFile(dirname);
            sortedFileIndex++;
            linesWritten = 0;
            completeFile = "";
        }
        postingsList.clear();
    }

    if(completeFile != "") {
        secondaryIndex += lastToken + "\n";
        writeToFile(dirname);
        writeToSecondaryIndex();
        linesWritten = 0;
        completeFile = "";
    }

    // Write to secondary index
    sortedFileIndex = 1;
    linesWritten = 0;

    for(int i = 0 ; i < numberOfFiles ; ++i) {
        files[i].close();
    }
    secondaryIndex = "";
}

// ----------------------------------------------------------------------------
int main(int argc, char *argv[]) {

    /* Fast IO */
    ios_base::sync_with_stdio(0);
    cin.tie(NULL);

    string numFiles(argv[1]);
    numOfLastTokens = 0;
    numberOfFiles = toInt(numFiles);
    linesWritten = 0;
    lastToken = "";
    sortedFileIndex = 1;

    indexDir = "index/";
    sortedIndexDir = "sorted/";
    secondaryIndexDir = "secondaryIndex/";
    completeFile = "";
    vector<string> sections{"body/",
                            "title/",
                            "references/",
                            "infobox/",
                            "links/",
                            "category/"};

    for(int i = 0 ; i < sections.size() ; ++i) {
        secIndFile.open(secondaryIndexDir + sections[i] + "index");
        merge(sections[i]);
        secIndFile.close();
    }
    return 0;
}
