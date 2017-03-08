## HadoopCryptoLedger security
Security of our solutions is important. Any peer review is very welcome so that organizations using HadoopCryptoLedger are never compromised.

### Report security issues
 You can report any security issues to zuinnote@gmail.com. Security issues have depending on their severity over bug fixes and/or work on new features. Please give the HadoopCryptoLedger developers and organizations using HadoopCryptoLedger some time to respond and we kindly ask you to treat all security issues privately to allow for this.
 
### Integrity
It is of key importance that all Bitcoin blocks can be fully parsed and made available to the application for analysis. This means we provide unit and integration tests to assure this. Default options are conservative and experimental options that may endanger this goal are deactivated. For example, we deactivate by default the option to split a file into several parts, because we cannot guarantee to 100% that all Bitcoin blocks can be identified properly in this case. Although we have not encountered such a scenario yet, we decided to make it impossible by deactivating this option by default.
