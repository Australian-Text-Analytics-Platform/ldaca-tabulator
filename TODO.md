
## Future directions

- Config files in the config folder is not generic. Can we do something about that, for example, removing that folder and do something with coding where we do not need config files. Discuss with Mike (SIH). It may not be possible but there may be a way to make it more generic. 

- Add docs to this package of how to use it. There is a branch for that named "docs_added" where you can add docs in there.

- We may not need the whole zip folder. Maybe someone is interested in downloading certain files instead of downloading the whole zip folder or someone may have access to less files based on authentication level. How can we download those files instead of downloading the whole folder? How can we incorporate this in this package.

- Currently, this package is downloading zip files and saving on disk. For example, if you give one url and run the code it will save all files in the disk. Give another url and it will download all the files for that corpus. This was needed and urgent for the app Alex developed. Can you do something where you replace this old corpus with the new one to not save many corpora on disk. Please also check this with Alex and Chao (SIH).

- Currently, this package is using repo from my account as `rocrate-tabular = { git = "https://github.com/AttiqUrRehmann/rocrate-tabular.git", rev = "optimise_entity_table" }` as code in rocrate was updated to make it faster but not merged into the original repo. This needs to be changed when mike merge pull request. Please check it with Mike.


