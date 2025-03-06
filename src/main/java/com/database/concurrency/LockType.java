package com.database.concurrency;

/**
 * Utility methods to track the relationships between different lock types.
 */
public enum LockType {
    S,   // shared
    X,   // exclusive
    IS,  // intention shared
    IX,  // intention exclusive
    SIX, // shared intention exclusive
    NL;  // no lock held

    /**
     * This method checks whether lock types A and B are compatible with
     * each other. If a transaction can hold lock type A on a resource
     * at the same time another transaction holds lock type B on the same
     * resource, the lock types are compatible.
     */
    public static boolean compatible(LockType a, LockType b) {
        if (a == null || b == null) {
            throw new NullPointerException("null lock type");
        }
        // TODO(proj4_part1): implement
        if(a.toString().equals("NL") || b.toString().equals("NL")){
            return true;
        } else if(a.toString().equals("S")){
            if(b.toString().equals("S") || b.toString().equals("IS")){
                return true;
            }
        } else if(a.toString().equals("X")){

        } else if(a.toString().equals("IS")){
            if(b.toString().equals("IS") || b.toString().equals("IX") || b.toString().equals("S") || b.toString().equals("SIX")){
                return true;
            }
        } else if(a.toString().equals("IX")){
            if(b.toString().equals("IS") || b.toString().equals("IX")){
                return true;
            }
        } else if(a.toString().equals("SIX")){
            if(b.toString().equals("IS")){
                return true;
            }
        }

        return false;
    }

    /**
     * This method returns the lock on the parent resource
     * that should be requested for a lock of type A to be granted.
     */
    public static LockType parentLock(LockType a) {
        if (a == null) {
            throw new NullPointerException("null lock type");
        }
        switch (a) {
        case S: return IS;
        case X: return IX;
        case IS: return IS;
        case IX: return IX;
        case SIX: return IX;
        case NL: return NL;
        default: throw new UnsupportedOperationException("bad lock type");
        }
    }

    /**
     * This method returns if parentLockType has permissions to grant a childLockType
     * on a child.
     */
    public static boolean canBeParentLock(LockType parentLockType, LockType childLockType) {
        if (parentLockType == null || childLockType == null) {
            throw new NullPointerException("null lock type");
        }
        // TODO(proj4_part1): implement
        if(parentLockType.toString().equals("IS")){
            return childLockType.toString().equals("S") || childLockType.toString().equals("IS") || childLockType.toString().equals("NL");
        } else if(parentLockType.toString().equals("IX")){
            return true;
        } else if(parentLockType.toString().equals("SIX")){
            return childLockType.toString().equals("X") || childLockType.toString().equals("IX") || childLockType.toString().equals("NL");
        } else if(parentLockType.toString().equals("NL")){
            return childLockType.toString().equals("NL");
        } else if(parentLockType.toString().equals("S") || parentLockType.toString().equals("X")){
            return childLockType.toString().equals("NL");
        }

        return false;
    }

    /**
     * This method returns whether a lock can be used for a situation
     * requiring another lock (e.g. an S lock can be substituted with
     * an X lock, because an X lock allows the transaction to do everything
     * the S lock allowed it to do).
     */
    public static boolean substitutable(LockType substitute, LockType required) {
        if (required == null || substitute == null) {
            throw new NullPointerException("null lock type");
        }
        // TODO(proj4_part1): implement
        if(substitute.toString().equals(required.toString())){
            return true;
        }
        if(required.toString().equals("NL")){
            return true;
        }
        if(substitute.toString().equals("X")){
            return true;
        } else if(substitute.toString().equals("S")){
            if(required.toString().equals("IS")){
                return true;
            }
        } else if(substitute.toString().equals("IX")){
            if(required.toString().equals("IS")){
                return true;
            }
        } else if(substitute.toString().equals("IS")){
            return false;
        } else if(substitute.toString().equals("SIX")){
            if(required.toString().equals("IS") || required.toString().equals("IX") || required.toString().equals("S")){
                return true;
            }
        } else if(substitute.toString().equals("NL")){
            return false;
        }

        return false;
    }

    /**
     * @return True if this lock is IX, IS, or SIX. False otherwise.
     */
    public boolean isIntent() {
        return this == LockType.IX || this == LockType.IS || this == LockType.SIX;
    }

    @Override
    public String toString() {
        switch (this) {
        case S: return "S";
        case X: return "X";
        case IS: return "IS";
        case IX: return "IX";
        case SIX: return "SIX";
        case NL: return "NL";
        default: throw new UnsupportedOperationException("bad lock type");
        }
    }
}

