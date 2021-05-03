# Breakdown of originalData Records:
|unit|record count|% of total|
|---|---|---|
|mm|672,811|49.9%|
|ft|675,027|50.1%|
|in|2|~|

## Output:
Total number of output Records: 12402
<br>Failure Found! ((41,30/12),CovFloat(27431.998,ArrayBuffer(24, 54, 56, 33, 58, 42)))
<br>Failure Found! ((20,30/12),CovFloat(27392.91,ArrayBuffer(24, 54, 58, 33, 42)))
<br>Failure Found! ((20,2016),CovFloat(27429.86,ArrayBuffer(24, 54, 58, 31, 42)))
<br>Failure Found! ((41,2016),CovFloat(27431.998,ArrayBuffer(24, 54, 56, 31, 58, 42)))
<br>Line Rankings: (LineNo, Suspicious Score)
<br>ArrayBuffer((31,0.6709600606126204), (58,0.6483970503634747), (24,0.5), (42,0.5), (54,0.5), (33,0.39847014205823744), (56,0.35378381463303277))
<br>FailureLines output map: Map(56 -> (11323,2), 31 -> (3040,2), 58 -> (6723,4), 54 -> (12398,4), 42 -> (12398,4), 33 -> (9358,2), 24 -> (12398,4))
<br>totalNumberOfPasses: 12398
<br>totalNumberOfFailures: 4
<br>Suggested line with bug: 31

### FailureLines output map:
|LineNo|NoOfPasses|NoOfFailures|%OfPasses|%OfFailures|
|---|---|---|---|---|
|56|11,323|2|91%|50%|
|31|3,040|2|25%|50%|
|58|6,723|4|54%|100%|
|54|12,398|4|100%|100%|
|42|12,398|4|100%|100%|
|33|9,358|2|75%|50%|
|24|12,398|4|100%|100%|

### Line Rankings:
|LineNo|Suspiciousness Score|
|---|---|
|31|0.6709600606126204|
|58|0.6483970503634747|
|24|0.5|
|42|0.5|
|54|0.5|
|33|0.39847014205823744|
|56|0.35378381463303277|

totalNumberOfPasses: 12398
totalNumberOfFailures: 4

### Result:
Suggested line with bug: 31

# Breakdown of moreMmData Records:
|unit|record count|% of total|
|---|---|---|
|mm|898,899|66.7%|
|ft|448,939|33.3%|
|in|2|~|

## Output:
Total number of output Records: 12402
<br>Failure Found! ((4,1946),CovFloat(47421.016,ArrayBuffer(24, 54, 56, 31, 58, 42)))
<br>Failure Found! ((4,30/12),CovFloat(22589.572,ArrayBuffer(24, 54, 56, 33, 58, 42)))
<br>Failure Found! ((4,30/11),CovFloat(47410.016,ArrayBuffer(24, 54, 56, 33, 58, 42)))
<br>Failure Found! ((4,2015),CovFloat(22620.887,ArrayBuffer(24, 54, 58, 31, 42)))
<br>Line Rankings: (LineNo, Suspicious Score)
<br>ArrayBuffer((58,0.7526711996114619), (31,0.6709600606126204), (24,0.5), (42,0.5), (54,0.5), (56,0.4343165417221327), (33,0.39847014205823744))
<br>FailureLines output map: Map(56 -> (12111,3), 31 -> (3040,2), 58 -> (4074,4), 54 -> (12398,4), 42 -> (12398,4), 33 -> (9358,2), 24 -> (12398,4))
<br>totalNumberOfPasses: 12398
<br>totalNumberOfFailures: 4
<br>Suggested line with bug: 58


### FailureLines output map:
|LineNo|NoOfPasses|NoOfFailures|%OfPasses|%OfFailures|
|---|---|---|---|---|
|56|12,111|3|98%|75%|
|31|3,040|2|25%|50%|
|58|4,074|4|33%|100%|
|54|12,398|4|100%|100%|
|42|12,398|4|100%|100%|
|33|9,358|2|75%|50%|
|24|12,398|4|100%|100%|
### Line Rankings:
|LineNo|Suspiciousness Score|
|---|---|
|58|0.7526711996114619|
|31|0.6709600606126204|
|24|0.5|
|42|0.5|
|54|0.5|
|56|0.4343165417221327|
|33|0.39847014205823744|

### Result:
Suggested line with bug: 58
