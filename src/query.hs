module Main where

import Options.Applicative (execParser)
import QueryProgram (commandLineParser, program)

main :: IO ()
main = execParser commandLineParser >>= program
