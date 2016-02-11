module Feitoria.DList where

newtype DList a = DList { unDList :: ([a] -> [a]) }

instance Show a => Show (DList a) where
    show = show . ($ []) . unDList

dlToList :: DList a -> [a]
dlToList = ($ []) . unDList

dlCons :: a -> DList a -> DList a
dlCons x (DList d) = DList $ (x:) . d

dlSnoc :: DList a -> a -> DList a
dlSnoc (DList d) x = DList $ d . (x:)

dlTail :: DList a -> DList a
dlTail (DList d) = DList $ tail . d

dlInit :: DList a -> DList a
dlInit (DList d) = DList $ init . d

dlApp :: DList a -> DList a -> DList a
dlApp (DList x) (DList y) = DList $ x . y
