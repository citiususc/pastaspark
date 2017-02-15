- Version 1.6.4:
	- ** Change:** Fix the copyright year to 2013-2016 

- Version 1.6.3:
    - **Change:**    Initial alignment now uses multiple cpus when available
    
- Version 1.6.2:
    - **Bug fix:**   When errors happen, PASTA stops right away (also checks for empty alignments)
    - **Bug fix:**   RAxML partition file was not updated according to masked sites
    - **Bug fix:**   `-gtr` was hard-coded for FastTree when model was empty; now `-gtr` is *NOT* automatically added when a configuration file is used and this enables using JK model.  (is _backward incompatible_ when a configuration file is used with FastTree as the tree estimator and the model is not set for FastTree in the configuration file)

- Version 1.6.1:
    - **Change:**    `run_seqtools.py` is redesigned. It does not use actions anymore (_backward incompatible_). 
    - **Change:**    `args` can be set in configuration files for each tool and these will be passed to respective tools
    - **Feature:**   Muscle is now available as an alignment tool

- Version 1.6 (is _backward incompatible_ for AA datasets):
    - **Change:**    We use opal now for pairwise mergers regardless of whether the datatype is AA or DNA.

- Version 1.5.3:
    - **Feature:**   A new script called `run_seqtools.py` is added that can be used to manipulate alignments
    - **Bug fix:**   For the initial alignment if all the masked sites were at the beginning, an error occurred. 

- Version 1.5.2:
    - **Bug fix:**   Sometimes, max subset size was set to 201. fixed the issue

- Version 1.5.1:
    - **Bug fix:**   RAxML post alignment step is fixed, and it uses the masked alignment
    - **Bug fix:**	Longest branch decomposition fixed - subsets of size 1 do not fail anymore
    - **Change:**	User set or automatically set subproblem size is forced to be at least 2
    - **Bug fix:**	FastTree is fixed to use (at most) the requested number of cpus
    - **Change:**	FastTree does not seem to be helped with more than 4 cpus. Max it at 4 cpus.
    - **Change:**	Mafft is moved to - Version 7.15
    - **Change:**	Opal is moved to - Version 2.1.2
    - **Change:**	Mafft is passed a --thread option to use multiple threads for starting alignment 
    - **Change:**	Mafft-linsi is used for upto 50,000 length

- Version 1.5 (is _backward incompatibile_):
    - **Major Change**	`--auto` option is not necessary anymore. Important settings are picked automatically based on the input file. These automatically picked defaults can be overwritten by the user. If `--auto` option is provided, it behaves like before (it overwrites the user provided options)
    - **Bug fix:**		Fasttree for DNA was missing `-gamma` option by default. Added!
    - **Change:**		`blind_after_total_iter` is set by default to 0, so that all changes are logged as accepted (before it switched to blind on first worse score)
    - **Change:**		pasta local home is now under `~/.pasta` instead of `~/.sate`
    - **Major Change:**	 file path config file is called `~/.pasta/pasta_tool_paths.cfg` instead of `~/.sate/sate_tool_paths.cfg`
    - **Change:**		If no job name is given, it will be set by default to `pastajob` instead of `satejob`
    - **Change:**		PASTA output (and logs) will say `PASTA INFO` instead of `SATe INFO`. More generally, the logs will say PASTA instead of SATe.
    - **Change:**		To turn on the debugging, now you need to use `PASTA_DEBUG` and `PASTA_LOGGING_LEVEL=DEBUG` instead of `SATE_DEBUG` and `SATE_LOGGING_LEVEL`
    - **Change:**		PASTA code changed such that in most places it uses `pasta` instead of `sate`. _Note_: the config file still has a section named `[sate]`. This was not renamed to maintain compatibility. 

- Version 1.4.1:
    - **Bug fix:** GUI was broken in 1.4.0
    - **Change:** Disable multi-locus checkbox. Multi-locus is not supported in PASTA. 

- Version 1.4.0:
    - **Major Change:** Uses HMMER to build the initial alignment if one is not given
    - **Change:** Initial tree also uses a masked alignment
