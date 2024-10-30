import React from 'react';

const baseNodeStyles = `
  p-4 rounded-xl shadow-lg border-2 min-w-[220px]
  transition-all duration-200 hover:shadow-xl
  backdrop-blur-sm backdrop-filter
  hover:scale-[1.02] cursor-pointer
`;

interface NodeWrapperProps {
  children: React.ReactNode;
  className: string;
  onClick?: () => void;
}

export const NodeWrapper = ({ children, className, onClick }: NodeWrapperProps) => (
  <div className={`${baseNodeStyles} ${className}`} onClick={onClick}>
    {children}
  </div>
);