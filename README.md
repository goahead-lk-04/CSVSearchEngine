# CSVSearchEngine
## Description
CSVSearchEngine is a framework for parsing, indexing, and querying CSV data with ease. 
It enables developers to perform complex searches and data operations on CSV files efficiently. 
Designed for flexibility, speed, and scalability, this framework is good for mobile development.

## Features
**Advanced Search Queries:** perform complex searches with conditions such as equality, range, and combined logical operators.
**Dynamic Indexing:** automatically indexes CSV data to ensure quick retrieval.
**Data Analysis Tools:** identify duplicates, missing values, or similar entries across datasets.

## Instalation
**SPM**
- in Xcode project go to **File > Add Package Dependencies**
- enter this repository URL
- select the latest version and click **Add Package**

**CocoaPods**
The framework is available on CocoaPods. Add the following to your Podfile:
`pod 'CSVSearchEngine'`
`pod install`

## Usage
To work with the file and run queries, firstly you need to index your csv file:
```swift
let filePath = "/path/to/your/file.csv"
let parser = CSVSearchEngine.CSVParser(filePath: filePath)
await parser.parseHeaders()
await parser.processRowsAsync(batchSize: 500)
parser.indexManager.loadRowPositionsFromFile()
```

## Planned Features
- comparing a bunch of csv files for row-matching
- searching in two and more files simultaneously
- add real-time analysis 
  
