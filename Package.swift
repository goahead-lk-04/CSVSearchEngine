// swift-tools-version: 5.10
// The swift-tools-version declares the minimum version of Swift required to build this package.

import PackageDescription

let package = Package(
    name: "CSVSearchEngine",
    products: [
        // Products define the executables and libraries a package produces, making them visible to other packages.
        .library(
            name: "CSVSearchEngine",
            targets: ["CSVSearchEngine"]),
    ],
//    dependencies: [
//            // Add this line
//            .package(url: "https://github.com/realm/SwiftLint.git", from: "0.45.0")
//        ],
    targets: [
        .target(
            name: "CSVSearchEngine"),
        .testTarget(
            name: "CSVSearchEngineTests",
            dependencies: ["CSVSearchEngine"]),
    ]
)
