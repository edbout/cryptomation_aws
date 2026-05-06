# Vi One-Page Reference

## Startup & Modes
| Command | Action |
|---------|--------|
| `vi filename` | Open file |
| `Esc` | Command mode |
| `i` | Insert before cursor |
| `a` | Append after cursor |
| `o`/`O` | New line below/above |
| `R` | Replace mode |

## Navigation
| Command | Action |
|---------|--------|
| `h j k l` | Left/down/up/right |
| `w/b` | Next/prev word |
| `0/$` | Line start/end |
| `gg/G` | File top/bottom |
| `:n` | Go to line n |
| `Ctrl-f/b` | Page down/up |

## Editing
| Command | Action |
|---------|--------|
| `x` | Delete char |
| `dd`/`5dd` | Delete line/5 lines |
| `dw` | Delete word |
| `cc` | Change line |
| `cw` | Change word |
| `r` | Replace char |
| `u`/`. ` | Undo/repeat |

## Copy/Paste
| Command | Action |
|---------|--------|
| `yy`/`3yy` | Copy line/3 lines |
| `p/P` | Paste after/before |
| `v` | Visual select |

## Search/Replace
| Command | Action |
|---------|--------|
| `/foo` | Search forward |
| `?foo` | Search backward |
| `n/N` | Next/prev match |
| `:%s/old/new/g` | Global replace |
| `:s/old/new/g` | Line replace |

## Save/Quit
| Command | Action |
|---------|--------|
| `:w` | Save |
| `:q` | Quit |
| `:wq`/`:x` | Save & quit |
| `:q!` | Quit no save |
| `ZZ` | Save & quit (fast) |

**Pro tip**: Prefix with numbers for repeats (like `5dd`). Print this—fits one page perfectly.