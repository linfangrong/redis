start_server {tags {"xset"}} {
    proc create_xset {key items} {
        r del $key
        foreach {score entry} $items {
            r xadd $key $score $entry
        }
    }

    proc basics {encoding} {
        if {$encoding == "ziplist"} {
            r config set xset-max-ziplist-entries 128
            r config set xset-max-ziplist-value 64
            r config set xset-finity 20
        } elseif {$encoding == "skiplist"} {
            r config set xset-max-ziplist-entries 0
            r config set xset-max-ziplist-value 0
            r config set xset-finity 20
        } else {
            puts "Unknown finite sorted set encoding"
            exit
        }

        test "Check encoding - $encoding" {
            r del xtmp
            r xadd xtmp 10 x
            assert_encoding $encoding xtmp
        }

        test "XSET basic XADD and score update - $encoding" {
            r del xtmp
            r xadd xtmp 10 x
            r xadd xtmp 20 y
            r xadd xtmp 30 z
            assert_equal {x y z} [r xrange xtmp 0 -1]

            r xadd xtmp 1 y
            assert_equal {y x z} [r xrange xtmp 0 -1]
        }

        test "XSET element can't be set to NaN with XADD - $encoding" {
            assert_error "*not*float*" {r xadd myxset nan abc}
        }

        test "XSET element can't be set to Nan with XINCRBY - $encoding" {
            assert_error "*not*float*" {r xincrby myxset nan abc}
        }

        test "XINCRBY calls leading to NaN result in error - $encoding" {
            r xincrby myxset +inf abc
            assert_error "*NaN*" {r xincrby myxset -inf abc}
        }

        test {XADD - Variadic version base case} {
            r del myxset
            list [r xadd myxset 10 a 20 b 30 c] [r xrange myxset 0 -1 withscores]
        } {3 {a 10 b 20 c 30}}

        test {XADD - Return value is the number of actually added items} {
            list [r xadd myxset 5 x 20 b 30 c] [r xrange myxset 0 -1 withscores]
        } {1 {x 5 a 10 b 20 c 30}}

        test {XADD - Variadic version does not add nothing on single parsing err} {
            r del myxset
            catch {r xadd myxset 10 a 20 b 30.badscore c} e
            assert_match {*ERR*not*float*} $e
            r exists myxset
        } {0}

        test {XADD - Variadic version will raise error on missing arg} {
            r del myxset
            catch {r xadd myxset 10 a 20 b 30 c 40} e
            assert_match {*ERR*syntax*} $e
        }

        test {XINCRBY does not work variadic even if shares XADD implementation} {
            r del myxset
            catch {r xincrby myxset 10 a 20 b 30 c} e
            assert_match {*ERR*single*} $e
        }

        test "XCARD basics - $encoding" {
            assert_equal 3 [r xcard xtmp]
            assert_equal 0 [r xcard xdoesntexist]
        }

        test "XREM removes key after last element is removed" {
            r del xtmp
            r xadd xtmp 10 x
            r xadd xtmp 20 y
            
            assert_equal 1 [r exists xtmp]
            assert_equal 0 [r xrem xtmp z]
            assert_equal 1 [r xrem xtmp y]
            assert_equal 1 [r xrem xtmp x]
            assert_equal 0 [r exists xtmp]
        }

        test "XREM variadic version" {
            r del xtmp
            r xadd xtmp 10 a 20 b 30 c
            assert_equal 2 [r xrem xtmp x y a b k]
            assert_equal 0 [r xrem xtmp foo bar]
            assert_equal 1 [r xrem xtmp c]
            r exists xtmp
        } {0}

        test "XREM variadic version -- remove elements after key deletion" {
            r del xtmp
            r xadd xtmp 10 a 20 b 30 c
            r xrem xtmp a b c d e f g
        } {3}

        test "XRANGE basics - $encoding" {
            r del xtmp
            r xadd xtmp 1 a
            r xadd xtmp 2 b
            r xadd xtmp 3 c
            r xadd xtmp 4 d

            assert_equal {a b c d} [r xrange xtmp 0 -1]
            assert_equal {a b c} [r xrange xtmp 0 -2]
            assert_equal {b c d} [r xrange xtmp 1 -1]
            assert_equal {b c} [r xrange xtmp 1 -2]
            assert_equal {c d} [r xrange xtmp -2 -1]
            assert_equal {c} [r xrange xtmp -2 -2]

            # out of range start index
            assert_equal {a b c} [r xrange xtmp -5 2]
            assert_equal {a b} [r xrange xtmp -5 1]
            assert_equal {} [r xrange xtmp 5 -1]
            assert_equal {} [r xrange xtmp 5 -2]

            # out of range end index
            assert_equal {a b c d} [r xrange xtmp 0 5]
            assert_equal {b c d} [r xrange xtmp 1 5]
            assert_equal {} [r xrange xtmp 0 -5]
            assert_equal {} [r xrange xtmp 1 -5]

            # withscores
            assert_equal {a 1 b 2 c 3 d 4} [r xrange xtmp 0 -1 withscores]
        }

        test "XREVRANGE basics - $encoding" {
            r del xtmp
            r xadd xtmp 1 a
            r xadd xtmp 2 b
            r xadd xtmp 3 c
            r xadd xtmp 4 d

            assert_equal {d c b a} [r xrevrange xtmp 0 -1]
            assert_equal {d c b} [r xrevrange xtmp 0 -2]
            assert_equal {c b a} [r xrevrange xtmp 1 -1]
            assert_equal {c b} [r xrevrange xtmp 1 -2]
            assert_equal {b a} [r xrevrange xtmp -2 -1]
            assert_equal {b} [r xrevrange xtmp -2 -2]

            # out of range start index
            assert_equal {d c b} [r xrevrange xtmp -5 2]
            assert_equal {d c} [r xrevrange xtmp -5 1]
            assert_equal {} [r xrevrange xtmp 5 -1]
            assert_equal {} [r xrevrange xtmp 5 -2]

            # out of range end index
            assert_equal {d c b a} [r xrevrange xtmp 0 5]
            assert_equal {c b a} [r xrevrange xtmp 1 5]
            assert_equal {} [r xrevrange xtmp 0 -5]
            assert_equal {} [r xrevrange xtmp 1 -5]

            # withscores
            assert_equal {d 4 c 3 b 2 a 1} [r xrevrange xtmp 0 -1 withscores]
        }

        test "XINCRBY - can create a new finite sorted set - $encoding" {
            r del xset
            r xincrby xset 1 foo
            assert_equal {foo} [r xrange xset 0 -1]
            assert_equal {foo 1} [r xrange xset 0 -1 withscores]
            assert_equal 1 [r xscore xset foo]
        }

        test "XINCRBY - increment and decrement - $encoding" {
            r xincrby xset 2 foo
            r xincrby xset 1 bar
            assert_equal {bar foo} [r xrange xset 0 -1]

            r xincrby xset 10 bar
            r xincrby xset -5 foo
            r xincrby xset -5 bar
            assert_equal {foo bar} [r xrange xset 0 -1]

            assert_equal {foo -2 bar 6} [r xrange xset 0 -1 withscores]
            assert_equal -2 [r xscore xset foo]
            assert_equal  6 [r xscore xset bar]
        }

        test "XADD - finity - $encoding" {
            r del xtmp
            r xadd xtmp 10 x
            r xadd xtmp 20 y
            r xadd xtmp 30 z
            assert_equal {x y z} [r xrange xtmp 0 -1]

            r xsetoptions xtmp finity 2
            assert_equal {y z} [r xrange xtmp 0 -1]

            assert_equal {y 20} [r xadd xtmp elements 40 x]
            assert_equal {2} [r xgetfinity xtmp]
            assert_equal {minscore} [r xgetpruning xtmp]
        }

        test "XADD - pruning - $encoding" {
            r del xtmp
            r xadd xtmp 10 x
            r xadd xtmp 20 y
            r xadd xtmp 30 z
            assert_equal {x y z} [r xrange xtmp 0 -1]

            r xsetoptions xtmp finity 2 pruning maxscore
            assert_equal {x y} [r xrange xtmp 0 -1]

            assert_equal {z 40} [r xadd xtmp elements 40 z]
            assert_equal {2} [r xgetfinity xtmp]
            assert_equal {maxscore} [r xgetpruning xtmp]
        }
    }

    basics ziplist
    basics skiplist

    proc stressers {encoding} {
        if {$encoding == "ziplist"} {
            r config set xset-max-ziplist-entries 256
            r config set xset-max-ziplist-value 64
            r config set xset-finity 200
            set elements 128
        } elseif {$encoding == "skiplist"} {
            r config set xset-max-ziplist-entries 0
            r config set xset-max-ziplist-value 0
            r config set xset-finity 2000
            if {$::accurate} {set elements 1000} else {set elements 100}
        } else {
            puts "Unknown finite sorted set encoding"
            exit
        }

        test "XSCORE - $encoding" {
            r del xscoretest
            set aux {}
            for {set i 0} {$i < $elements} {incr i} {
                set score [expr rand()]
                lappend aux $score
                r xadd xscoretest $score $i
            }

            assert_encoding $encoding xscoretest
            for {set i 0} {$i < $elements} {incr i} {
                assert_equal [lindex $aux $i] [r xscore xscoretest $i]
            }
        }

        test "XSCORE after a DEBUG RELOAD - $encoding" {
            r del xscoretest
            set aux {}
            for {set i 0} {$i < $elements} {incr i} {
                set score [expr rand()]
                lappend aux $score
                r xadd xscoretest $score $i
            }

            r debug reload
            assert_encoding $encoding xscoretest
            for {set i 0} {$i < $elements} {incr i} {
                assert_equal [lindex $aux $i] [r xscore xscoretest $i]
            }
        }

        test "XSET sorting stresser - $encoding" {
            set delta 0
            for {set test 0} {$test < 2} {incr test} {
                unset -nocomplain auxarray
                array set auxarray {}
                set auxlist {}
                r del myxset
                for {set i 0} {$i < $elements} {incr i} {
                    if {$test == 0} {
                        set score [expr rand()]
                    } else {
                        set score [expr int(rand()*10)]
                    }
                    set auxarray($i) $score
                    r xadd myxset $score $i
                    # Random update
                    if {[expr rand()] < .2} {
                        set j [expr int(rand()*1000)]
                        if {$test == 0} {
                            set score [expr rand()]
                        } else {
                            set score [expr int(rand()*10)]
                        }
                        set auxarray($j) $score
                        r xadd myxset $score $j
                    }
                }
                foreach {item score} [array get auxarray] {
                    lappend auxlist [list $score $item]
                }
                set sorted [lsort -command zlistAlikeSort $auxlist]
                set auxlist {}
                foreach x $sorted {
                    lappend auxlist [lindex $x 1]
                }

                assert_encoding $encoding myxset
                set fromredis [r xrange myxset 0 -1]
                set delta 0
                for {set i 0} {$i < [llength $fromredis]} {incr i} {
                    if {[lindex $fromredis $i] != [lindex $auxlist $i]} {
                        incr delta
                    }
                }
            }
            assert_equal 0 $delta
        }

    }

    tags {"slow"} {
        stressers ziplist
        stressers skiplist
    }
}
