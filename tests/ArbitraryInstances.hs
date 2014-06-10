--
-- Copyright © 2013-2014 Anchor Systems, Pty Ltd and Others
--
-- The code in this file, and the program it is a part of, is
-- made available to you by its authors as open source software:
-- you can redistribute it and/or modify it under the terms of
-- the 3-clause BSD licence.
--

{-# OPTIONS_GHC -fno-warn-orphans #-}


module ArbitraryInstances
(
) where

import Control.Applicative ((<$>), (<*>))
import Test.QuickCheck.Arbitrary
import Test.QuickCheck.Gen
import Test.QuickCheck.Instances ()
import Vaultaire.Types

instance Arbitrary Address where
    arbitrary = Address <$> arbitrary

instance Arbitrary SourceDict where
    arbitrary = do
        attempt <- arbitrary
        either (const arbitrary) return $ makeSourceDict attempt

instance Arbitrary ContentsOperation where
    arbitrary = oneof [ return ContentsListRequest
                      , return GenerateNewAddress
                      , UpdateSourceTag <$> arbitrary <*> arbitrary
                      , RemoveSourceTag <$> arbitrary <*> arbitrary ]

instance Arbitrary ContentsResponse where
    arbitrary = oneof [ RandomAddress  <$> arbitrary
                      , ContentsListEntry <$> arbitrary <*> arbitrary
                      , return EndOfContentsList
                      , return UpdateSuccess
                      , return RemoveSuccess ]

instance Arbitrary WriteResult where
    arbitrary = oneof [ return InvalidWriteOrigin, return OnDisk ]

instance Arbitrary ReadStream where
    arbitrary = oneof [ return InvalidReadOrigin
                      , SimpleBurst <$> arbitrary
                      , ExtendedBurst <$> arbitrary
                      , return EndOfStream ]


